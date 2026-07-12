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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexer;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.table.source.DeletionVectorTestUtils.commitDeletionVectors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VectorSearchBuilder} using test-only brute-force vector index. */
public class VectorSearchBuilderTest extends TableTestBase {

    private static final String VECTOR_FIELD_NAME = "vec";
    private static final int DIMENSION = 2;

    @Override
    protected Schema schemaDefault() {
        return vectorSchemaBuilder(VECTOR_FIELD_NAME).build();
    }

    protected Schema.Builder vectorSchemaBuilder(String vectorFieldName) {
        return withVectorSchemaOptions(
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(vectorFieldName, new ArrayType(DataTypes.FLOAT())));
    }

    protected Schema.Builder hybridVectorSchemaBuilder() {
        return withVectorSchemaOptions(
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("title_vec", new ArrayType(DataTypes.FLOAT()))
                        .column("body_vec", new ArrayType(DataTypes.FLOAT())));
    }

    protected Schema.Builder withVectorSchemaOptions(Schema.Builder builder) {
        return builder.option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option("test.vector.dimension", String.valueOf(DIMENSION))
                .option("test.vector.metric", "l2");
    }

    @Test
    public void testHybridSearchBuilderExposesRouteBuilders() throws Exception {
        catalog.createTable(
                identifier("hybrid_vector_builder_table"),
                hybridVectorSchemaBuilder().build(),
                false);
        FileStoreTable table = getTable(identifier("hybrid_vector_builder_table"));

        float[][] titleVectors = {{1.0f, 0.0f}, {0.9f, 0.1f}, {0.0f, 1.0f}};
        float[][] bodyVectors = {{0.0f, 1.0f}, {0.1f, 0.9f}, {1.0f, 0.0f}};
        writeTwoVectorColumns(table, titleVectors, bodyVectors);
        buildAndCommitIndex(table, "title_vec", titleVectors);
        buildAndCommitIndex(table, "body_vec", bodyVectors);

        HybridSearchBuilder builder =
                table.newHybridSearchBuilder()
                        .addVectorRoute("title_vec", new float[] {1.0f, 0.0f}, 2)
                        .addVectorRoute("body_vec", new float[] {0.0f, 1.0f}, 2, 2.0f)
                        .withLimit(2)
                        .withWeightedScoreRanker();
        List<HybridSearchBuilder.Route> routes = builder.routeBuilders();

        assertThat(routes).hasSize(2);

        List<HybridSearchBuilder.RouteResult> routeResults = new ArrayList<>();
        for (HybridSearchBuilder.Route route : routes) {
            routeResults.add(
                    builder.toRouteResult(route, route.vectorSearchBuilder().executeLocal()));
        }
        ScoredGlobalIndexResult ranked = builder.rank(routeResults);

        assertThat(ranked.results().getIntCardinality()).isEqualTo(2);
        assertThat(ranked.results()).contains(1L);
    }

    @Test
    public void testVectorSearchEndToEnd() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {
            {1.0f, 0.0f},
            {0.95f, 0.1f},
            {0.1f, 0.95f},
            {0.98f, 0.05f},
            {0.0f, 1.0f},
            {0.05f, 0.98f}
        };

        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        // Query vector close to (1.0, 0.0) - should return rows 0,1,3
        float[] queryVector = {0.85f, 0.15f};
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();

        // Read using the search result
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Integer> ids = new ArrayList<>();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).isNotEmpty();
        assertThat(ids.size()).isLessThanOrEqualTo(3);
        // Row 0 (1.0, 0.0) should be the closest to query (0.85, 0.15)
        assertThat(ids).contains(0);
    }

    @Test
    public void testVectorSearchExcludesDeletedIndexedRows() throws Exception {
        catalog.createTable(
                identifier("vector_search_deleted_indexed_rows"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("vector_search_deleted_indexed_rows"));

        float[][] vectors = {{0.0f, 0.0f}, {1.0f, 0.0f}, {2.0f, 0.0f}, {3.0f, 0.0f}};
        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);
        commitDeletionVectors(table, 0L, 1L);

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result.results().getLongCardinality()).isEqualTo(2);
        assertThat(result.results()).contains(2L, 3L);
        assertThat(result.results()).doesNotContain(0L, 1L);
        assertThat(readIds(table, result)).containsExactly(2, 3);
    }

    @Test
    public void testBatchVectorSearchExcludesDeletedIndexedRows() throws Exception {
        catalog.createTable(
                identifier("batch_vector_search_deleted_indexed_rows"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("batch_vector_search_deleted_indexed_rows"));

        float[][] vectors = {{0.0f, 0.0f}, {1.0f, 0.0f}, {2.0f, 0.0f}, {3.0f, 0.0f}};
        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);
        commitDeletionVectors(table, 0L, 3L);

        List<GlobalIndexResult> results =
                table.newBatchVectorSearchBuilder()
                        .withVectors(new float[][] {{0.0f, 0.0f}, {3.0f, 0.0f}})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeBatchLocal();

        assertThat(results).hasSize(2);
        assertThat(results.get(0).results()).contains(1L);
        assertThat(results.get(0).results()).doesNotContain(0L);
        assertThat(results.get(1).results()).contains(2L);
        assertThat(results.get(1).results()).doesNotContain(3L);
    }

    @Test
    public void testVectorSearchWithCosineMetric() throws Exception {
        // Create a table with cosine metric
        catalog.createTable(
                identifier("cosine_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option("test.vector.metric", "cosine")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("cosine_table"));

        float[][] vectors = {
            {1.0f, 0.0f},
            {0.707f, 0.707f},
            {0.0f, 1.0f},
        };

        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        // Query along x-axis: closest should be (1,0), then (0.707,0.707), then (0,1)
        float[] queryVector = {1.0f, 0.0f};
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).hasSize(2);
        // Row 0 (1,0) has cosine similarity = 1.0, row 1 (0.707,0.707) ~ 0.707
        assertThat(ids).contains(0, 1);
    }

    @Test
    public void testVectorSearchEmptyResult() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        // Write data but no index - should return empty result
        float[][] vectors = {{1.0f, 0.0f}, {0.0f, 1.0f}};
        writeVectors(table, vectors);

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result.results().isEmpty()).isTrue();
    }

    @Test
    public void testFullModeRawOnlyUsesConfiguredMetric() throws Exception {
        catalog.createTable(
                identifier("full_search_raw_only_cosine_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full")
                        .option("test.vector.metric", "cosine")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("full_search_raw_only_cosine_table"));

        float[][] vectors = {{100.0f, 0.0f}, {0.9f, 0.1f}};
        writeVectors(table, vectors);

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result.results()).containsExactly(0L);
    }

    @Test
    public void testVectorSearchFullModeScansUnindexedData() throws Exception {
        catalog.createTable(
                identifier("full_search_cosine_table"),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(VECTOR_FIELD_NAME, new ArrayType(DataTypes.FLOAT()))
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full")
                        .option("test.vector.dimension", String.valueOf(DIMENSION))
                        .option("test.vector.metric", "cosine")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("full_search_cosine_table"));

        float[][] vectors = {
            {0.0f, 1.0f},
            {0.1f, 0.9f},
            {0.5f, 0.0f},
            {0.99f, 0.01f}
        };
        writeVectors(table, vectors);

        buildAndCommitVectorIndex(table, new float[][] {vectors[0], vectors[1]}, new Range(0, 1));

        TestVectorGlobalIndexer.resetMetricCalls();
        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .newVectorScan()
                        .scan();
        assertThat(indexVectorSearchSplits(plan.splits())).hasSize(1);
        assertThat(rawVectorSearchSplits(plan.splits())).hasSize(1);
        assertThat(rawVectorSearchSplits(plan.splits()).get(0).rowRanges())
                .containsExactly(new Range(2, 3));

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result.results()).containsExactlyInAnyOrder(2L, 3L);
        assertThat(TestVectorGlobalIndexer.metricCalls()).isGreaterThan(0);
    }

    @Test
    public void testVectorSearchFastModeSkipsUnindexedDataByDefault() throws Exception {
        catalog.createTable(
                identifier("fast_search_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME).build(),
                false);
        FileStoreTable table = getTable(identifier("fast_search_table"));

        float[][] vectors = {
            {0.0f, 1.0f},
            {0.1f, 0.9f},
            {1.0f, 0.0f},
            {0.95f, 0.05f}
        };
        writeVectors(table, vectors);

        buildAndCommitVectorIndex(table, new float[][] {vectors[0], vectors[1]}, new Range(0, 1));

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result.results()).doesNotContain(2L, 3L);
    }

    @Test
    public void testVectorSearchFullModeScansFilteredUnindexedData() throws Exception {
        catalog.createTable(
                identifier("full_search_filtered_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("full_search_filtered_table"));

        float[][] vectors = {
            {0.0f, 1.0f},
            {0.1f, 0.9f},
            {1.0f, 0.0f},
            {0.95f, 0.05f}
        };
        writeVectors(table, vectors);

        Range indexedRange = new Range(0, 1);
        buildAndCommitVectorIndex(table, new float[][] {vectors[0], vectors[1]}, indexedRange);
        buildAndCommitBTreeIndex(table, new int[] {0, 1}, indexedRange);

        Predicate filter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 2);
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(filter)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results()).containsExactlyInAnyOrder(2L, 3L);
    }

    @Test
    public void testVectorSearchRawSearchUsesScalarPreFilter() throws Exception {
        catalog.createTable(
                identifier("raw_search_scalar_prefilter_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("raw_search_scalar_prefilter_table"));

        float[][] vectors = {
            {100.0f, 0.0f},
            {200.0f, 0.0f},
            {1.0f, 0.0f},
            {2.0f, 0.0f}
        };
        writeVectors(table, vectors);

        buildAndCommitVectorIndex(table, new float[][] {vectors[0], vectors[1]}, new Range(0, 1));
        buildAndCommitBTreeIndex(table, new int[] {2, 3}, new Range(2, 3));

        // Let scalar global index evaluate id >= 3, but let the final raw row filter pass through.
        // Without raw pre-filtering, raw topK would pick row 2 because it is closer to the query.
        Predicate filter = globalIndexOnlyIdGreaterOrEqual(table, 3);
        VectorSearchBuilder builder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(filter);
        VectorScan.Plan plan = builder.newVectorScan().scan();
        assertThat(rawVectorSearchSplits(plan.splits()).get(0).scalarIndexFiles()).isNotEmpty();

        GlobalIndexResult result = builder.newVectorRead().read(plan);
        assertThat(result.results()).containsExactly(3L);
    }

    @Test
    public void testVectorSearchTopKLimit() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = new float[20][];
        for (int i = 0; i < 20; i++) {
            vectors[i] = new float[] {(float) Math.cos(i * 0.3), (float) Math.sin(i * 0.3)};
        }

        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        // Search with limit=5
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(5)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids.size()).isLessThanOrEqualTo(5);
    }

    @Test
    public void testVectorSearchThreadsOptions() throws Exception {
        catalog.createTable(
                identifier("options_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option("test.vector.required-option.key", "ivf.nprobe")
                        .option("test.vector.required-option.value", "16")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("options_table"));

        float[][] vectors = {{1.0f, 0.0f}, {0.0f, 1.0f}};
        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withOption("ivf.nprobe", "16")
                        .executeLocal();

        assertThat(result.results().isEmpty()).isFalse();
    }

    @Test
    public void testVectorSearchRefineFactorReranksIndexCandidates() throws Exception {
        catalog.createTable(
                identifier("refine_factor_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(TestVectorGlobalIndexer.OPT_REVERSE_SCORE, "true")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("refine_factor_table"));

        float[][] vectors = {{0.0f, 0.0f}, {10.0f, 0.0f}, {20.0f, 0.0f}};
        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        GlobalIndexResult approximate =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();
        assertThat(approximate.results()).containsExactly(2L);

        GlobalIndexResult refined =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withOption(
                                TestVectorGlobalIndexerFactory.IDENTIFIER + ".refine_factor", "3")
                        .executeLocal();
        assertThat(refined.results()).containsExactly(0L);
        assertThat(readIds(table, refined)).containsExactly(0);

        List<GlobalIndexResult> batchRefined =
                table.newBatchVectorSearchBuilder()
                        .withVectors(new float[][] {{0.0f, 0.0f}, {20.0f, 0.0f}})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withOption(
                                TestVectorGlobalIndexerFactory.IDENTIFIER + ".refine_factor", "3")
                        .executeBatchLocal();
        assertThat(batchRefined).hasSize(2);
        assertThat(batchRefined.get(0).results()).containsExactly(0L);
        assertThat(batchRefined.get(1).results()).containsExactly(2L);
    }

    @Test
    public void testRawRefineReadTypeContainsOnlyVectorAndRowId() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        Predicate idFilter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 1);

        ExposingDataEvolutionVectorRead read = new ExposingDataEvolutionVectorRead(table, idFilter);

        assertThat(read.rawReadType(false).getFieldNames())
                .containsExactly(VECTOR_FIELD_NAME, SpecialFields.ROW_ID.name());
        assertThat(read.rawReadType(true).getFieldNames())
                .containsExactly(VECTOR_FIELD_NAME, "id", SpecialFields.ROW_ID.name());
    }

    @Test
    public void testRawCandidateSearchScoresOnlyCandidateBitmap() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeVectors(table, new float[][] {{0.0f, 0.0f}, {10.0f, 0.0f}, {20.0f, 0.0f}});

        RoaringNavigableMap64 candidates = new RoaringNavigableMap64();
        candidates.add(1L);

        ExposingDataEvolutionVectorRead read = new ExposingDataEvolutionVectorRead(table, null);
        ScoredGlobalIndexResult result =
                read.rawCandidateSearch(
                        Collections.singletonList(new Range(0, 2)),
                        candidates,
                        new float[] {0.0f, 0.0f});

        assertThat(result.results()).containsExactly(1L);
    }

    @Test
    public void testBatchRefineReadsUnionCandidatesOnceAndScoresPerQuery() {
        RecordingBatchVectorRead read = new RecordingBatchVectorRead();

        ScoredGlobalIndexResult[] reranked =
                read.rerank(
                        new ScoredGlobalIndexResult[] {
                            scoredResult(1.0f, 0L, 2L), scoredResult(1.0f, 1L, 2L)
                        });

        assertThat(read.rawReadCount).isEqualTo(1);
        assertThat(read.rawReadRanges).containsExactly(new Range(0, 2));
        assertThat(read.rawCandidates).containsExactly(0L, 1L, 2L);
        assertThat(reranked[0].results()).containsExactly(2L);
        assertThat(reranked[1].results()).containsExactly(2L);
    }

    @Test
    public void testVectorSearchRefineFactorValidation() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {{0.0f, 0.0f}, {1.0f, 0.0f}};
        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        assertThatThrownBy(
                        () ->
                                table.newVectorSearchBuilder()
                                        .withVector(new float[] {0.0f, 0.0f})
                                        .withLimit(1)
                                        .withVectorColumn(VECTOR_FIELD_NAME)
                                        .withOption("refine_factor", "0")
                                        .executeLocal())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refine factor must be positive");
    }

    @Test
    public void testVectorSearchWithMultipleIndexFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] allVectors = {
            {1.0f, 0.0f}, // row 0 - close to (1,0)
            {0.95f, 0.1f}, // row 1 - close to (1,0)
            {0.1f, 0.95f}, // row 2 - far from (1,0)
            {0.98f, 0.05f}, // row 3 - close to (1,0)
            {0.0f, 1.0f}, // row 4 - far from (1,0)
            {0.05f, 0.98f} // row 5 - far from (1,0)
        };

        writeVectors(table, allVectors);

        // Build two separate index files covering different row ranges
        buildAndCommitMultipleIndexFiles(table, allVectors);

        // Query vector close to (1.0, 0.0) - results should span across both index files
        float[] queryVector = {0.85f, 0.15f};
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).isNotEmpty();
        assertThat(ids.size()).isLessThanOrEqualTo(3);
        // Row 0 (1.0,0.0), Row 1 (0.95,0.1), Row 3 (0.98,0.05) are closest to query
        // Row 0 is in the first index file, Row 3 is in the second index file
        assertThat(ids).contains(0);
        assertThat(ids).containsAnyOf(1, 3);
    }

    @Test
    public void testVectorSearchWithPartitionFilter() throws Exception {
        // Create a partitioned table
        catalog.createTable(
                identifier("partitioned_table"),
                withVectorSchemaOptions(
                                Schema.newBuilder()
                                        .column("pt", DataTypes.INT())
                                        .column("id", DataTypes.INT())
                                        .column(VECTOR_FIELD_NAME, new ArrayType(DataTypes.FLOAT()))
                                        .partitionKeys("pt"))
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("partitioned_table"));

        // Partition 1: vectors close to (1,0)
        float[][] pt1Vectors = {{1.0f, 0.0f}, {0.95f, 0.1f}, {0.98f, 0.05f}};
        // Partition 2: vectors close to (0,1)
        float[][] pt2Vectors = {{0.0f, 1.0f}, {0.1f, 0.95f}, {0.05f, 0.98f}};

        writePartitionedVectors(table, 1, pt1Vectors);
        writePartitionedVectors(table, 2, pt2Vectors);

        RowType partitionType = RowType.of(DataTypes.INT());
        InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
        BinaryRow partition1 = serializer.toBinaryRow(GenericRow.of(1)).copy();
        BinaryRow partition2 = serializer.toBinaryRow(GenericRow.of(2)).copy();

        // Build and commit indexes with non-overlapping row ranges
        buildAndCommitPartitionedIndex(table, pt1Vectors, partition1, new Range(0, 2));
        buildAndCommitPartitionedIndex(table, pt2Vectors, partition2, new Range(3, 5));

        float[] queryVector = {0.9f, 0.1f};

        // Search with partition filter for partition 1 only
        PartitionPredicate partFilter1 =
                PartitionPredicate.fromMultiple(
                        partitionType, Collections.singletonList(partition1));
        GlobalIndexResult result1 =
                table.newVectorSearchBuilder()
                        .withPartitionFilter(partFilter1)
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result1).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result1.results().isEmpty()).isFalse();
        // Row IDs should be within partition 1's row range [0, 2]
        for (long rowId : result1.results()) {
            assertThat(rowId).isBetween(0L, 2L);
        }

        // Search with partition filter for partition 2 only
        PartitionPredicate partFilter2 =
                PartitionPredicate.fromMultiple(
                        partitionType, Collections.singletonList(partition2));
        GlobalIndexResult result2 =
                table.newVectorSearchBuilder()
                        .withPartitionFilter(partFilter2)
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result2).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result2.results().isEmpty()).isFalse();
        // Row IDs should be within partition 2's row range [3, 5]
        for (long rowId : result2.results()) {
            assertThat(rowId).isBetween(3L, 5L);
        }

        // Search without partition filter - returns results from both partitions
        GlobalIndexResult resultAll =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(6)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(resultAll.results().getIntCardinality())
                .isEqualTo(
                        result1.results().getIntCardinality()
                                + result2.results().getIntCardinality());
    }

    @Test
    public void testVectorSearchPartitionFilterAndExtractedFilterAreConjunctive() throws Exception {
        catalog.createTable(
                identifier("partitioned_filter_table"),
                withVectorSchemaOptions(
                                Schema.newBuilder()
                                        .column("pt", DataTypes.INT())
                                        .column("id", DataTypes.INT())
                                        .column(VECTOR_FIELD_NAME, new ArrayType(DataTypes.FLOAT()))
                                        .partitionKeys("pt"))
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("partitioned_filter_table"));

        float[][] pt1Vectors = {{1.0f, 0.0f}, {0.95f, 0.1f}};
        float[][] pt2Vectors = {{0.0f, 1.0f}, {0.1f, 0.95f}};

        writePartitionedVectors(table, 1, pt1Vectors);
        writePartitionedVectors(table, 2, pt2Vectors);

        RowType partitionType = RowType.of(DataTypes.INT());
        InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
        BinaryRow partition1 = serializer.toBinaryRow(GenericRow.of(1)).copy();
        BinaryRow partition2 = serializer.toBinaryRow(GenericRow.of(2)).copy();

        buildAndCommitPartitionedIndex(table, pt1Vectors, partition1, new Range(0, 1));
        buildAndCommitPartitionedIndex(table, pt2Vectors, partition2, new Range(2, 3));

        PartitionPredicate partitionFilter =
                PartitionPredicate.fromMultiple(
                        partitionType, Collections.singletonList(partition1));
        Predicate matchingPartitionFilter = new PredicateBuilder(table.rowType()).equal(0, 1);
        Predicate extractedPartitionFilter = new PredicateBuilder(table.rowType()).equal(0, 2);

        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withPartitionFilter(partitionFilter)
                        .withFilter(extractedPartitionFilter)
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .newVectorScan()
                        .scan();

        assertThat(plan.splits()).isEmpty();

        VectorScan.Plan reverseOrderPlan =
                table.newVectorSearchBuilder()
                        .withFilter(extractedPartitionFilter)
                        .withPartitionFilter(partitionFilter)
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .newVectorScan()
                        .scan();

        assertThat(reverseOrderPlan.splits()).isEmpty();

        VectorScan.Plan batchPlan =
                table.newBatchVectorSearchBuilder()
                        .withPartitionFilter(partitionFilter)
                        .withFilter(extractedPartitionFilter)
                        .withVectors(new float[][] {new float[] {1.0f, 0.0f}})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .newVectorScan()
                        .scan();

        assertThat(batchPlan.splits()).isEmpty();

        VectorScan.Plan reverseOrderBatchPlan =
                table.newBatchVectorSearchBuilder()
                        .withFilter(extractedPartitionFilter)
                        .withPartitionFilter(partitionFilter)
                        .withVectors(new float[][] {new float[] {1.0f, 0.0f}})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .newVectorScan()
                        .scan();

        assertThat(reverseOrderBatchPlan.splits()).isEmpty();

        GlobalIndexResult matchingResult =
                table.newVectorSearchBuilder()
                        .withFilter(matchingPartitionFilter)
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();
        assertResultRowsBetween(matchingResult, 0, 1);

        GlobalIndexResult matchingResultWithPartitionFilter =
                table.newVectorSearchBuilder()
                        .withPartitionFilter(partitionFilter)
                        .withFilter(matchingPartitionFilter)
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();
        assertResultRowsBetween(matchingResultWithPartitionFilter, 0, 1);

        List<GlobalIndexResult> matchingBatchResults =
                table.newBatchVectorSearchBuilder()
                        .withFilter(matchingPartitionFilter)
                        .withVectors(new float[][] {new float[] {1.0f, 0.0f}})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeBatchLocal();
        assertThat(matchingBatchResults).hasSize(1);
        assertResultRowsBetween(matchingBatchResults.get(0), 0, 1);

        List<GlobalIndexResult> matchingBatchResultsWithPartitionFilter =
                table.newBatchVectorSearchBuilder()
                        .withPartitionFilter(partitionFilter)
                        .withFilter(matchingPartitionFilter)
                        .withVectors(new float[][] {new float[] {1.0f, 0.0f}})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeBatchLocal();
        assertThat(matchingBatchResultsWithPartitionFilter).hasSize(1);
        assertResultRowsBetween(matchingBatchResultsWithPartitionFilter.get(0), 0, 1);
    }

    @Test
    public void testScanPartialRangeIntersection() throws Exception {
        catalog.createTable(
                identifier("full_search_partial_scalar_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("full_search_partial_scalar_table"));

        // Write 10 rows
        float[][] allVectors = new float[10][];
        for (int i = 0; i < 10; i++) {
            allVectors[i] = new float[] {(float) Math.cos(i * 0.3), (float) Math.sin(i * 0.3)};
        }
        writeVectors(table, allVectors);

        // Build ONE vector index covering full range [0,9]
        buildAndCommitVectorIndex(table, allVectors, new Range(0, 9));

        // Build ONE btree index covering partial range [3,7]
        buildAndCommitBTreeIndex(table, new int[] {3, 4, 5, 6, 7}, new Range(3, 7));

        // DataEvolutionVectorScan should attach scalar index because [3,7] intersects [0,9]
        Predicate idFilter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 5);
        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(5)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter)
                        .newVectorScan()
                        .scan();

        assertThat(indexVectorSearchSplits(plan.splits())).hasSize(1);
        assertThat(rawVectorSearchSplits(plan.splits())).hasSize(1);
        IndexVectorSearchSplit split = indexVectorSearchSplits(plan.splits()).get(0);
        assertThat(split.rowRangeStart()).isEqualTo(0);
        assertThat(split.rowRangeEnd()).isEqualTo(9);
        assertThat(split.vectorIndexFiles()).isNotEmpty();
        // Scalar index [3,7] intersects vector range [0,9], so it is attached.
        assertThat(split.scalarIndexFiles()).isNotEmpty();
        assertThat(rawVectorSearchSplits(plan.splits()).get(0).rowRanges())
                .containsExactly(new Range(0, 2), new Range(8, 9));

        // Read with pre-filter: btree covers [3,7], while rows outside that scalar index coverage
        // are searched through raw splits and filtered by the raw table read.
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(5)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter)
                        .newVectorRead()
                        .read(plan);

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();

        assertThat(result.results()).containsAnyOf(5L, 6L, 7L);
        assertThat(result.results()).doesNotContain(0L, 1L, 2L);
    }

    @Test
    public void testPreFilterMatchesZeroRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {
            {1.0f, 0.0f},
            {0.95f, 0.1f},
            {0.0f, 1.0f},
            {0.1f, 0.95f}
        };
        writeVectors(table, vectors);

        Range range = new Range(0, 3);
        buildAndCommitVectorIndex(table, vectors, range);
        buildAndCommitBTreeIndex(table, new int[] {0, 1, 2, 3}, range);

        // Filter id > 100: btree covers ids 0-3, so preFilter matches zero rows
        Predicate impossibleFilter = new PredicateBuilder(table.rowType()).greaterThan(0, 100);
        VectorSearchBuilder searchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(4)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(impossibleFilter);

        VectorScan.Plan plan = searchBuilder.newVectorScan().scan();
        assertThat(plan.splits()).hasSize(1);
        // Scalar index is attached since field matches filter
        assertThat(((IndexVectorSearchSplit) plan.splits().get(0)).scalarIndexFiles()).isNotEmpty();

        // Read: preFilter returns empty bitmap, so vector search returns no results.
        GlobalIndexResult result = searchBuilder.newVectorRead().read(plan);
        assertThat(result.results().isEmpty()).isTrue();
    }

    @Test
    public void testPartialScalarPreFilterMustNotDropUnindexedScalarRows() throws Exception {
        catalog.createTable(
                identifier("full_search_partial_scalar_unindexed_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("full_search_partial_scalar_unindexed_table"));

        float[][] vectors = new float[10][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = new float[] {Math.abs(i - 8), 0.0f};
        }
        writeVectors(table, vectors);

        buildAndCommitVectorIndex(table, vectors, new Range(0, 9));
        buildAndCommitBTreeIndex(table, new int[] {3, 4, 5, 6, 7}, new Range(3, 7));

        Predicate idFilter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 8);
        VectorSearchBuilder searchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter);

        VectorScan.Plan vectorPlan = searchBuilder.newVectorScan().scan();
        GlobalIndexResult result = searchBuilder.newVectorRead().read(vectorPlan);
        assertThat(result.results()).contains(8L);

        ReadBuilder readBuilder = table.newReadBuilder().withFilter(idFilter);
        TableScan.Plan readPlan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(readPlan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).containsExactly(8);
    }

    @Test
    public void testFullModeFilterWithoutScalarIndexMustNotLetVectorIndexPolluteTopK()
            throws Exception {
        catalog.createTable(
                identifier("full_search_no_scalar_index_filter_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("full_search_no_scalar_index_filter_table"));

        float[][] vectors = new float[10][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = new float[] {Math.abs(i), 0.0f};
        }
        writeVectors(table, vectors);

        buildAndCommitVectorIndex(table, vectors, new Range(0, 9));

        Predicate idFilter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 8);
        VectorSearchBuilder searchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter);

        GlobalIndexResult result =
                searchBuilder.newVectorRead().read(searchBuilder.newVectorScan().scan());

        ReadBuilder readBuilder = table.newReadBuilder().withFilter(idFilter);
        TableScan.Plan readPlan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(readPlan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).containsExactly(8);
    }

    @Test
    public void testFastModePartialScalarPreFilterOnlyUsesIndexedRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = new float[10][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = new float[] {Math.abs(i - 8), 0.0f};
        }
        writeVectors(table, vectors);

        buildAndCommitVectorIndex(table, vectors, new Range(0, 9));
        buildAndCommitBTreeIndex(table, new int[] {3, 4, 5, 6, 7}, new Range(3, 7));

        Predicate idFilter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 8);
        VectorSearchBuilder searchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter);

        GlobalIndexResult result =
                searchBuilder.newVectorRead().read(searchBuilder.newVectorScan().scan());
        assertThat(result.results().isEmpty()).isTrue();
    }

    @Test
    public void testVectorSearchRequiresVectorColumnAsPrimaryField() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {{1.0f, 0.0f}, {0.0f, 1.0f}};
        writeVectors(table, vectors);
        buildAndCommitVectorIndexWithFields(
                table,
                vectors,
                new Range(0, 1),
                Arrays.asList(table.rowType().getField("id"), table.rowType().getField("vec")));

        VectorSearchBuilder searchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME);

        VectorScan.Plan plan = searchBuilder.newVectorScan().scan();
        assertThat(plan.splits()).isEmpty();
        assertThat(searchBuilder.executeLocal().results().isEmpty()).isTrue();
    }

    @Test
    public void testVectorSearchSplitSerialization() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {{1.0f, 0.0f}, {0.0f, 1.0f}};
        writeVectors(table, vectors);

        Range range = new Range(0, 1);
        buildAndCommitVectorIndex(table, vectors, range);
        buildAndCommitBTreeIndex(table, new int[] {0, 1}, range);

        Predicate filter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 0);
        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(filter)
                        .newVectorScan()
                        .scan();

        assertThat(plan.splits()).hasSize(1);
        IndexVectorSearchSplit original = (IndexVectorSearchSplit) plan.splits().get(0);

        // Serialize
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(original);
        }

        // Deserialize
        IndexVectorSearchSplit deserialized;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            deserialized = (IndexVectorSearchSplit) in.readObject();
        }

        // Verify all fields match
        assertThat(deserialized.rowRangeStart()).isEqualTo(original.rowRangeStart());
        assertThat(deserialized.rowRangeEnd()).isEqualTo(original.rowRangeEnd());
        assertThat(deserialized.vectorIndexFiles()).hasSize(original.vectorIndexFiles().size());
        assertThat(deserialized.scalarIndexFiles()).hasSize(original.scalarIndexFiles().size());
        for (int i = 0; i < original.vectorIndexFiles().size(); i++) {
            assertThat(deserialized.vectorIndexFiles().get(i).fileName())
                    .isEqualTo(original.vectorIndexFiles().get(i).fileName());
        }
        for (int i = 0; i < original.scalarIndexFiles().size(); i++) {
            assertThat(deserialized.scalarIndexFiles().get(i).fileName())
                    .isEqualTo(original.scalarIndexFiles().get(i).fileName());
        }
    }

    @Test
    public void testBatchVectorSearch() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {
            {1.0f, 0.0f},
            {0.95f, 0.1f},
            {0.1f, 0.95f},
            {0.98f, 0.05f},
            {0.0f, 1.0f},
            {0.05f, 0.98f}
        };

        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        float[][] queryVectors = {
            {1.0f, 0.0f},
            {0.0f, 1.0f},
            {0.7f, 0.7f}
        };

        BatchVectorSearchBuilder searchBuilder =
                table.newBatchVectorSearchBuilder()
                        .withVectors(queryVectors)
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME);

        List<GlobalIndexResult> results =
                searchBuilder.newBatchVectorRead().readBatch(searchBuilder.newVectorScan().scan());

        assertThat(results).hasSize(3);

        // Query 0 near (1,0): should find rows 0 (1,0) and 3 (0.98,0.05)
        assertThat(results.get(0).results().isEmpty()).isFalse();
        ReadBuilder rb0 = table.newReadBuilder();
        List<Integer> ids0 = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                rb0.newRead()
                        .createReader(rb0.newScan().withGlobalIndexResult(results.get(0)).plan())) {
            reader.forEachRemaining(row -> ids0.add(row.getInt(0)));
        }
        assertThat(ids0).contains(0);

        // Query 1 near (0,1): should find rows 4 (0,1) and 5 (0.05,0.98)
        assertThat(results.get(1).results().isEmpty()).isFalse();
        ReadBuilder rb1 = table.newReadBuilder();
        List<Integer> ids1 = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                rb1.newRead()
                        .createReader(rb1.newScan().withGlobalIndexResult(results.get(1)).plan())) {
            reader.forEachRemaining(row -> ids1.add(row.getInt(0)));
        }
        assertThat(ids1).contains(4);
    }

    @Test
    public void testBatchVectorSearchFullModeScansUnindexedData() throws Exception {
        catalog.createTable(
                identifier("batch_full_search_table"),
                vectorSchemaBuilder(VECTOR_FIELD_NAME)
                        .option(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("batch_full_search_table"));

        float[][] vectors = {
            {0.0f, 1.0f},
            {0.1f, 0.9f},
            {1.0f, 0.0f},
            {0.95f, 0.05f}
        };
        writeVectors(table, vectors);
        buildAndCommitVectorIndex(table, new float[][] {vectors[0], vectors[1]}, new Range(0, 1));

        List<GlobalIndexResult> results =
                table.newBatchVectorSearchBuilder()
                        .withVectors(new float[][] {{1.0f, 0.0f}, {0.0f, 1.0f}})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeBatchLocal();

        assertThat(results).hasSize(2);
        assertThat(results.get(0).results()).containsExactly(2L);
        assertThat(results.get(1).results()).containsExactly(0L);
    }

    @Test
    public void testBatchVectorSearchWithMultipleIndexFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] allVectors = {
            {1.0f, 0.0f},
            {0.95f, 0.1f},
            {0.1f, 0.95f},
            {0.98f, 0.05f},
            {0.0f, 1.0f},
            {0.05f, 0.98f}
        };

        writeVectors(table, allVectors);
        buildAndCommitMultipleIndexFiles(table, allVectors);

        List<GlobalIndexResult> results =
                table.newBatchVectorSearchBuilder()
                        .withVectors(new float[][] {{0.85f, 0.15f}, {0.0f, 1.0f}})
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeBatchLocal();

        assertThat(results).hasSize(2);

        List<Integer> ids0 = readIds(table, results.get(0));
        assertThat(ids0.size()).isLessThanOrEqualTo(3);
        assertThat(ids0).contains(0, 3);

        List<Integer> ids1 = readIds(table, results.get(1));
        assertThat(ids1.size()).isLessThanOrEqualTo(3);
        assertThat(ids1).contains(4, 5);
    }

    @Test
    public void testBatchSingleVector() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {
            {1.0f, 0.0f},
            {0.95f, 0.1f},
            {0.0f, 1.0f},
            {0.98f, 0.05f}
        };

        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        float[] queryVector = {0.9f, 0.1f};

        GlobalIndexResult singleResult =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        List<GlobalIndexResult> batchResults =
                table.newBatchVectorSearchBuilder()
                        .withVectors(new float[][] {queryVector})
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeBatchLocal();

        assertThat(batchResults).hasSize(1);
        assertThat(batchResults.get(0).results().getIntCardinality())
                .isEqualTo(singleResult.results().getIntCardinality());

        for (long rowId : singleResult.results()) {
            assertThat(batchResults.get(0).results().contains(rowId)).isTrue();
        }
    }

    // ====================== Helper methods ======================

    private void writeVectors(FileStoreTable table, float[][] vectors) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(i, new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void writeTwoVectorColumns(
            FileStoreTable table, float[][] titleVectors, float[][] bodyVectors) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < titleVectors.length; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                new GenericArray(titleVectors[i]),
                                new GenericArray(bodyVectors[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private List<Integer> readIds(FileStoreTable table, GlobalIndexResult result) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        return ids;
    }

    private void assertResultRowsBetween(GlobalIndexResult result, long start, long end) {
        assertThat(result.results().isEmpty()).isFalse();
        for (long rowId : result.results()) {
            assertThat(rowId).isBetween(start, end);
        }
    }

    private void buildAndCommitIndex(FileStoreTable table, float[][] vectors) throws Exception {
        buildAndCommitIndex(table, VECTOR_FIELD_NAME, vectors);
    }

    private static class ExposingDataEvolutionVectorRead extends DataEvolutionVectorRead {

        private ExposingDataEvolutionVectorRead(FileStoreTable table, Predicate filter) {
            super(
                    table,
                    null,
                    filter,
                    1,
                    table.rowType().getField(VECTOR_FIELD_NAME),
                    new float[] {0.0f, 0.0f},
                    null);
        }

        private RowType rawReadType(boolean includeFilter) {
            return rawSearchReadType(includeFilter);
        }

        private ScoredGlobalIndexResult rawCandidateSearch(
                List<Range> rawRowRanges, RoaringNavigableMap64 candidates, float[] queryVector) {
            return readRawCandidateSearch(rawRowRanges, candidates, "l2", queryVector, false);
        }
    }

    private static class RecordingBatchVectorRead extends BatchVectorReadImpl {

        private int rawReadCount;
        private List<Range> rawReadRanges;
        private RoaringNavigableMap64 rawCandidates;
        private final Map<Long, float[]> rawVectors = new HashMap<>();

        private RecordingBatchVectorRead() {
            super(
                    null,
                    null,
                    null,
                    1,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[][] {{0.0f}, {10.0f}},
                    refineOptions());
            rawVectors.put(0L, new float[] {10.0f});
            rawVectors.put(1L, new float[] {0.0f});
            rawVectors.put(2L, new float[] {5.0f});
        }

        private ScoredGlobalIndexResult[] rerank(ScoredGlobalIndexResult[] results) {
            return maybeRerankIndexedBatchResults(results, "ivf-pq", null);
        }

        @Override
        protected Map<Long, float[]> readRawVectors(
                List<Range> rawRowRanges, RoaringNavigableMap64 candidates, boolean includeFilter) {
            rawReadCount++;
            rawReadRanges = rawRowRanges;
            rawCandidates = candidates;
            assertThat(includeFilter).isFalse();

            Map<Long, float[]> result = new HashMap<>();
            for (long rowId : candidates) {
                result.put(rowId, rawVectors.get(rowId));
            }
            return result;
        }
    }

    private static Map<String, String> refineOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("refine_factor", "2");
        options.put("test.vector.metric", "l2");
        return options;
    }

    private static ScoredGlobalIndexResult scoredResult(float score, long... rowIds) {
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        for (long rowId : rowIds) {
            rows.add(rowId);
        }
        return ScoredGlobalIndexResult.create(rows, rowId -> score);
    }

    private void buildAndCommitIndex(FileStoreTable table, String fieldName, float[][] vectors)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(fieldName);

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
        }
        List<ResultEntry> entries = writer.finish();

        Range rowRange = new Range(0, vectors.length - 1);
        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitMultipleIndexFiles(FileStoreTable table, float[][] vectors)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD_NAME);
        int mid = vectors.length / 2;

        // Build first index file covering rows [0, mid)
        GlobalIndexSingleColumnWriter writer1 =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (int i = 0; i < mid; i++) {
            writer1.write(vectors[i], i);
        }
        List<ResultEntry> entries1 = writer1.finish();
        Range rowRange1 = new Range(0, mid - 1);
        List<IndexFileMeta> indexFiles1 =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange1,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries1);

        // Build second index file covering rows [mid, end)
        GlobalIndexSingleColumnWriter writer2 =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (int i = mid; i < vectors.length; i++) {
            writer2.write(vectors[i], i - mid);
        }
        List<ResultEntry> entries2 = writer2.finish();
        Range rowRange2 = new Range(mid, vectors.length - 1);
        List<IndexFileMeta> indexFiles2 =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange2,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries2);

        // Combine all index files and commit together
        List<IndexFileMeta> allIndexFiles = new ArrayList<>();
        allIndexFiles.addAll(indexFiles1);
        allIndexFiles.addAll(indexFiles2);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(allIndexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void writePartitionedVectors(FileStoreTable table, int partition, float[][] vectors)
            throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(partition, i, new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    @Test
    public void testVectorSearchWithBTreePreFilter() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        // Write 10 rows: ids 0-9 with vectors
        // Rows 0-4: vectors near (1,0)
        // Rows 5-9: vectors near (0,1)
        float[][] allVectors = {
            {1.0f, 0.0f}, // row 0
            {0.95f, 0.1f}, // row 1
            {0.98f, 0.05f}, // row 2
            {0.9f, 0.15f}, // row 3
            {0.85f, 0.2f}, // row 4
            {0.0f, 1.0f}, // row 5
            {0.1f, 0.95f}, // row 6
            {0.05f, 0.98f}, // row 7
            {0.15f, 0.9f}, // row 8
            {0.2f, 0.85f} // row 9
        };
        writeVectors(table, allVectors);

        Range range1 = new Range(0, 4);
        Range range2 = new Range(5, 9);

        // Build two vector index files for each range
        buildAndCommitVectorIndex(
                table,
                new float[][] {
                    allVectors[0], allVectors[1], allVectors[2], allVectors[3], allVectors[4]
                },
                range1);
        buildAndCommitVectorIndex(
                table,
                new float[][] {
                    allVectors[5], allVectors[6], allVectors[7], allVectors[8], allVectors[9]
                },
                range2);

        // Build two btree indexes on 'id' field for each range
        buildAndCommitBTreeIndex(table, new int[] {0, 1, 2, 3, 4}, range1);
        buildAndCommitBTreeIndex(table, new int[] {5, 6, 7, 8, 9}, range2);

        // --- Test DataEvolutionVectorScan: verify splits contain scalar index files ---
        Predicate idFilter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 5);
        VectorSearchBuilder searchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.1f, 0.9f})
                        .withLimit(5)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter);

        VectorScan.Plan plan = searchBuilder.newVectorScan().scan();
        assertThat(plan.splits()).isNotEmpty();
        // Every split should have vector index files
        for (IndexVectorSearchSplit split : indexVectorSearchSplits(plan.splits())) {
            assertThat(split.vectorIndexFiles()).isNotEmpty();
        }
        // At least one split should have scalar (btree) index files
        long scalarCount =
                indexVectorSearchSplits(plan.splits()).stream()
                        .filter(s -> !s.scalarIndexFiles().isEmpty())
                        .count();
        assertThat(scalarCount).isGreaterThan(0);

        // --- Test DataEvolutionVectorRead: pre-filter should narrow results ---
        // Query vector near (0,1) with filter id >= 5
        // Without filter: rows 5,6,7,8,9 are closest
        // With filter id >= 5: btree pre-filter restricts to rows 5-9
        GlobalIndexResult resultWithFilter = searchBuilder.newVectorRead().read(plan);
        assertThat(resultWithFilter).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(resultWithFilter.results().isEmpty()).isFalse();
        for (long rowId : resultWithFilter.results()) {
            assertThat(rowId).isBetween(5L, 9L);
        }

        // Compare with no-filter search (should include results from both ranges)
        GlobalIndexResult resultNoFilter =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.1f, 0.9f})
                        .withLimit(10)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();
        assertThat(resultNoFilter.results().getIntCardinality())
                .isGreaterThan(resultWithFilter.results().getIntCardinality());
    }

    private void buildAndCommitVectorIndex(FileStoreTable table, float[][] vectors, Range rowRange)
            throws Exception {
        buildAndCommitVectorIndexWithFields(
                table,
                vectors,
                rowRange,
                Collections.singletonList(table.rowType().getField(VECTOR_FIELD_NAME)));
    }

    private void buildAndCommitVectorIndexWithFields(
            FileStoreTable table, float[][] vectors, Range rowRange, List<DataField> indexFields)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD_NAME);

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        indexFields,
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitBTreeIndex(FileStoreTable table, int[] ids, Range rowRange)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField idField = table.rowType().getField("id");

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, BTreeGlobalIndexerFactory.IDENTIFIER, idField, options);
        for (int id : ids) {
            long relativeRowId = id - rowRange.from;
            writer.write(id, relativeRowId);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        idField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitPartitionedIndex(
            FileStoreTable table, float[][] vectors, BinaryRow partition, Range rowRange)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD_NAME);

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private List<IndexVectorSearchSplit> indexVectorSearchSplits(List<VectorSearchSplit> splits) {
        List<IndexVectorSearchSplit> indexSplits = new ArrayList<>();
        for (VectorSearchSplit split : splits) {
            if (split instanceof IndexVectorSearchSplit) {
                indexSplits.add((IndexVectorSearchSplit) split);
            }
        }
        return indexSplits;
    }

    private List<RawVectorSearchSplit> rawVectorSearchSplits(List<VectorSearchSplit> splits) {
        List<RawVectorSearchSplit> rawSplits = new ArrayList<>();
        for (VectorSearchSplit split : splits) {
            if (split instanceof RawVectorSearchSplit) {
                rawSplits.add((RawVectorSearchSplit) split);
            }
        }
        return rawSplits;
    }

    private Predicate globalIndexOnlyIdGreaterOrEqual(FileStoreTable table, int literal) {
        DataField idField = table.rowType().getField("id");
        return new GlobalIndexOnlyLeafPredicate(
                new FieldTransform(new FieldRef(0, idField.name(), idField.type())),
                GreaterOrEqual.INSTANCE,
                Collections.singletonList(literal));
    }

    private static class GlobalIndexOnlyLeafPredicate extends LeafPredicate {

        private static final long serialVersionUID = 1L;

        private GlobalIndexOnlyLeafPredicate(
                Transform transform, LeafFunction function, List<Object> literals) {
            super(transform, function, literals);
        }

        @Override
        public boolean test(InternalRow row) {
            return true;
        }

        @Override
        public LeafPredicate copyWithNewInputs(List<Object> newInputs) {
            return new GlobalIndexOnlyLeafPredicate(
                    transform().copyWithNewInputs(newInputs), function(), literals());
        }
    }
}
