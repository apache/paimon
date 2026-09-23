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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Vector search with a row filter must rank an exact match set: candidate-only scalar index answers
 * (BTree contains / partially indexed conjunctions) let a non-matching but closer row take a top-k
 * slot, and the engine-side filter cannot recover the matching row afterwards.
 */
public class VectorSearchRowFilterExactnessTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return schemaBuilder(true).build();
    }

    /**
     * {@code refine} sets {@code global-index.filter.refine-from-data}, which defaults to false.
     */
    private static Schema.Builder schemaBuilder(boolean refine) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("vec", new ArrayType(DataTypes.FLOAT()))
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(
                        CoreOptions.GLOBAL_INDEX_FILTER_REFINE_FROM_DATA.key(),
                        Boolean.toString(refine))
                .option("test.vector.dimension", "2")
                .option("test.vector.metric", "l2");
    }

    private FileStoreTable createTable(String name, Schema.Builder schema) throws Exception {
        Identifier identifier = identifier(name);
        catalog.createTable(identifier, schema.build(), false);
        return getTable(identifier);
    }

    private static org.apache.paimon.utils.RoaringNavigableMap64 search(
            FileStoreTable table, Predicate filter, int limit) {
        return table.newVectorSearchBuilder()
                .withVector(new float[] {1.0f, 0.0f})
                .withVectorColumn("vec")
                .withLimit(limit)
                .withFilter(filter)
                .executeLocal()
                .results();
    }

    @Test
    public void testContainsOnBTreeColumnRanksOnlyMatchingRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        // Row 0 is the nearest neighbour of the query but does not contain "zeta".
        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names);

        Predicate containsZeta =
                new PredicateBuilder(table.rowType()).contains(1, BinaryString.fromString("zeta"));
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(containsZeta)
                        .executeLocal();

        assertThat(result.results()).containsExactly(1L);
    }

    @Test
    public void testPartiallyIndexedConjunctionRanksOnlyMatchingRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitIdBTreeIndex(table, names.length);

        // `id` is indexed, `name` is not: the evaluator drops the second conjunct.
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate partiallyIndexed =
                PredicateBuilder.and(
                        builder.greaterOrEqual(0, 0),
                        builder.equal(1, BinaryString.fromString("beta zeta")));
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(partiallyIndexed)
                        .executeLocal();

        assertThat(result.results()).containsExactly(1L);
    }

    @Test
    public void testOtherCandidateOnlyOperatorsAndEmptyRefinement() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names);

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        assertThat(search(table, builder.endsWith(1, BinaryString.fromString("zeta")), 1))
                .containsExactly(1L);
        assertThat(search(table, builder.like(1, BinaryString.fromString("%zeta")), 1))
                .containsExactly(1L);
        // The candidate set is every non-null row; refining it leaves nothing.
        assertThat(search(table, builder.contains(1, BinaryString.fromString("omega")), 3))
                .isEmpty();
        // An OR with a branch no index can evaluate is not narrowed by the index at all.
        assertThat(
                        search(
                                table,
                                PredicateBuilder.or(
                                        builder.equal(0, 1),
                                        builder.equal(1, BinaryString.fromString("x"))),
                                1))
                .doesNotContain(0L, 2L);
    }

    @Test
    public void testCandidatesAreRefinedPerIndexRange() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        // Two vector index ranges; the matching row of each range is not its nearest neighbour.
        String[] names = {"alpha", "beta zeta", "gamma", "delta zeta", "epsilon", "eta"};
        float[][] vectors = {
            {1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}, {0.6f, -0.8f}, {1.0f, 0.1f}, {0.0f, -1.0f}
        };
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors, new Range(0, 2));
        buildAndCommitVectorIndex(table, vectors, new Range(3, 5));
        buildAndCommitNameBTreeIndex(table, names);

        Predicate containsZeta =
                new PredicateBuilder(table.rowType()).contains(1, BinaryString.fromString("zeta"));
        assertThat(search(table, containsZeta, 2)).containsExactlyInAnyOrder(1L, 3L);
        assertThat(search(table, containsZeta, 10)).containsExactlyInAnyOrder(1L, 3L);
    }

    @Test
    public void testRefinementCombinedWithDeletionVectors() throws Exception {
        Identifier identifier = identifier("vector_refine_dv");
        catalog.createTable(
                identifier,
                schemaBuilder(true)
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier);
        String[] names = {"alpha", "beta zeta", "gamma zeta"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names);
        DeletionVectorTestUtils.commitDeletionVectors(table, 1L);

        Predicate containsZeta =
                new PredicateBuilder(table.rowType()).contains(1, BinaryString.fromString("zeta"));
        assertThat(search(table, containsZeta, 1)).containsExactly(2L);
    }

    @Test
    public void testBatchAndHybridVectorSearchRefineCandidates() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names);
        Predicate containsZeta =
                new PredicateBuilder(table.rowType()).contains(1, BinaryString.fromString("zeta"));

        List<GlobalIndexResult> batch =
                table.newBatchVectorSearchBuilder()
                        .withVectors(new float[][] {{1.0f, 0.0f}, {0.0f, 1.0f}})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(containsZeta)
                        .executeBatchLocal();
        assertThat(batch).hasSize(2);
        assertThat(batch.get(0).results()).containsExactly(1L);
        assertThat(batch.get(1).results()).containsExactly(1L);

        GlobalIndexResult hybrid =
                table.newHybridSearchBuilder()
                        .addVectorRoute("vec", new float[] {1.0f, 0.0f}, 1)
                        .withFilter(containsZeta)
                        .withLimit(1)
                        .executeLocal();
        assertThat(hybrid.results()).containsExactly(1L);
    }

    // ---------------------------------------------------------------------------------------
    //  global-index.filter.refine-from-data = false (the default)
    // ---------------------------------------------------------------------------------------

    @Test
    public void testRefineFromDataIsOffByDefault() throws Exception {
        FileStoreTable table =
                createTable(
                        "vector_refine_default",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("vec", new ArrayType(DataTypes.FLOAT()))
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                                .option("test.vector.dimension", "2")
                                .option("test.vector.metric", "l2"));
        assertThat(table.coreOptions().globalIndexFilterRefineFromData()).isFalse();

        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names);

        // Candidates are excluded: never the non-matching row 0, and no data is read.
        Predicate containsZeta =
                new PredicateBuilder(table.rowType()).contains(1, BinaryString.fromString("zeta"));
        assertThat(search(table, containsZeta, 1)).isEmpty();
    }

    @Test
    public void testRefineDisabledExcludesEveryCandidateOnlyAnswer() throws Exception {
        FileStoreTable table = createTable("vector_refine_off", schemaBuilder(false));
        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names);

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        for (Predicate candidateOnly :
                Arrays.asList(
                        builder.contains(1, BinaryString.fromString("zeta")),
                        builder.endsWith(1, BinaryString.fromString("zeta")),
                        builder.like(1, BinaryString.fromString("%zeta")))) {
            assertThat(search(table, candidateOnly, 3)).as(candidateOnly.toString()).isEmpty();
        }

        // Exact answers on the same index are unaffected by the option.
        assertThat(search(table, builder.equal(1, BinaryString.fromString("beta zeta")), 1))
                .containsExactly(1L);
        assertThat(search(table, builder.startsWith(1, BinaryString.fromString("beta")), 1))
                .containsExactly(1L);
        assertThat(
                        search(
                                table,
                                builder.in(
                                        1,
                                        Arrays.asList(
                                                BinaryString.fromString("beta zeta"),
                                                BinaryString.fromString("gamma"))),
                                2))
                .containsExactlyInAnyOrder(1L, 2L);

        // A conjunction with a member no index can evaluate is a superset as well: excluded.
        FileStoreTable partial = createTable("vector_refine_off_partial", schemaBuilder(false));
        write(partial, names, vectors);
        buildAndCommitVectorIndex(partial, vectors);
        buildAndCommitIdBTreeIndex(partial, names.length);
        PredicateBuilder partialBuilder = new PredicateBuilder(partial.rowType());
        assertThat(
                        search(
                                partial,
                                PredicateBuilder.and(
                                        partialBuilder.greaterOrEqual(0, 0),
                                        partialBuilder.equal(
                                                1, BinaryString.fromString("beta zeta"))),
                                3))
                .isEmpty();
        assertThat(search(partial, partialBuilder.greaterOrEqual(0, 1), 1)).containsExactly(1L);
    }

    @Test
    public void testRefineDisabledAppliesToBatchAndHybridSearch() throws Exception {
        FileStoreTable table = createTable("vector_refine_off_batch", schemaBuilder(false));
        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names);
        Predicate containsZeta =
                new PredicateBuilder(table.rowType()).contains(1, BinaryString.fromString("zeta"));

        List<GlobalIndexResult> batch =
                table.newBatchVectorSearchBuilder()
                        .withVectors(new float[][] {{1.0f, 0.0f}, {0.0f, 1.0f}})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(containsZeta)
                        .executeBatchLocal();
        assertThat(batch).hasSize(2);
        assertThat(batch.get(0).results().isEmpty()).isTrue();
        assertThat(batch.get(1).results().isEmpty()).isTrue();

        GlobalIndexResult hybrid =
                table.newHybridSearchBuilder()
                        .addVectorRoute("vec", new float[] {1.0f, 0.0f}, 1)
                        .withFilter(containsZeta)
                        .withLimit(1)
                        .executeLocal();
        assertThat(hybrid.results().isEmpty()).isTrue();
    }

    @Test
    public void testRefineDisabledKeepsUnindexedColumnsOnTheDataPathInFullMode() throws Exception {
        // The option gates only candidate refinement; rows whose filter column has no index at
        // all still follow scalar-index.search-mode.
        FileStoreTable table =
                createTable(
                        "vector_refine_off_full",
                        schemaBuilder(false)
                                .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "full"));
        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);

        Predicate nameFilter =
                new PredicateBuilder(table.rowType())
                        .equal(1, BinaryString.fromString("beta zeta"));
        assertThat(search(table, nameFilter, 1)).containsExactly(1L);
    }

    @ParameterizedTest(name = "scalar-index.search-mode={0}")
    @ValueSource(strings = {"fast", "full"})
    public void testFilterOnUnindexedColumn(String scalarMode) throws Exception {
        // No scalar index at all: the filter cannot be answered by any index.
        Identifier identifier = identifier("vector_unindexed_" + scalarMode);
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("vec", new ArrayType(DataTypes.FLOAT()))
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), scalarMode)
                        .option("test.vector.dimension", "2")
                        .option("test.vector.metric", "l2")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier);
        String[] names = {"alpha", "beta zeta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);

        Predicate nameFilter =
                new PredicateBuilder(table.rowType())
                        .equal(1, BinaryString.fromString("beta zeta"));
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(nameFilter)
                        .executeLocal();

        // Never a non-matching row; full mode must find the matching one from the data.
        assertThat(result.results()).doesNotContain(0L, 2L);
        if (scalarMode.equals("full")) {
            assertThat(result.results()).containsExactly(1L);
        }
    }

    private void write(FileStoreTable table, String[] names, float[][] vectors) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < names.length; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString(names[i]),
                                new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void buildAndCommitVectorIndex(FileStoreTable table, float[][] vectors)
            throws Exception {
        buildAndCommitVectorIndex(table, vectors, new Range(0, vectors.length - 1));
    }

    /** Indexes {@code vectors[rowRange]} as one index file; local ids start at 0. */
    private void buildAndCommitVectorIndex(FileStoreTable table, float[][] vectors, Range rowRange)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField("vec");
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (long rowId = rowRange.from; rowId <= rowRange.to; rowId++) {
            writer.write(vectors[(int) rowId], rowId - rowRange.from);
        }
        commitIndex(
                table,
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        writer.finish()));
    }

    private void buildAndCommitNameBTreeIndex(FileStoreTable table, String[] names)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField nameField = table.rowType().getField("name");
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, BTreeGlobalIndexerFactory.IDENTIFIER, nameField, options);
        // The btree writer needs sorted keys.
        Integer[] order = new Integer[names.length];
        for (int i = 0; i < names.length; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> names[a].compareTo(names[b]));
        for (int rowId : order) {
            writer.write(BinaryString.fromString(names[rowId]), rowId);
        }
        commitIndex(
                table,
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        new Range(0, names.length - 1),
                        nameField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        writer.finish()));
    }

    private void buildAndCommitIdBTreeIndex(FileStoreTable table, int rowCount) throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField idField = table.rowType().getField("id");
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, BTreeGlobalIndexerFactory.IDENTIFIER, idField, options);
        for (int i = 0; i < rowCount; i++) {
            writer.write(i, i);
        }
        commitIndex(
                table,
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        new Range(0, rowCount - 1),
                        idField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        writer.finish()));
    }

    private static void commitIndex(FileStoreTable table, List<IndexFileMeta> indexFiles)
            throws Exception {
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        DataIncrement.indexIncrement(indexFiles),
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }
}
