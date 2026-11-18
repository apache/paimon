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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LowerTransform;
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
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Vector search with a row filter must rank an exact match set: candidate-only scalar index answers
 * (BTree contains / partially indexed conjunctions) let a non-matching but closer row take a top-k
 * slot, and the engine-side filter cannot recover the matching row afterwards.
 */
public class VectorSearchRowFilterExactnessTest extends TableTestBase {

    @Test
    public void testPostFilterKeepsIndexedRangesOffRawPath() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        String[] names = new String[128];
        float[][] vectors = new float[128][2];
        for (int i = 0; i < names.length; i++) {
            names[i] = i >= 8 ? "match" : "other";
            vectors[i][0] = i;
        }
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        Predicate filter =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("match"));
        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0, 0})
                        .withVectorColumn("vec")
                        .withLimit(2)
                        .withFilter(filter)
                        .newVectorScan()
                        .scan();
        DataEvolutionVectorRead read =
                new DataEvolutionVectorRead(
                        table,
                        null,
                        filter,
                        2,
                        table.rowType().getField("vec"),
                        new float[] {0, 0},
                        Collections.emptyMap()) {
                    @Override
                    protected ScoredGlobalIndexResult readRawSearch(
                            List<Range> ranges,
                            org.apache.paimon.utils.RoaringNavigableMap64 include,
                            GlobalIndexer indexer,
                            float[] query) {
                        throw new AssertionError(
                                "Indexed candidates must be filtered without raw scoring");
                    }
                };
        assertThat(read.read(plan).results()).containsExactly(8L, 9L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"fast", "full", "detail"})
    public void testPostFilterFactorAllowsShortResultsWithoutRawRecovery(String scalarMode)
            throws Exception {
        FileStoreTable table =
                createTable(
                        "post_filter_" + scalarMode,
                        schemaBuilder(true)
                                .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), scalarMode)
                                .option(
                                        CoreOptions.VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR
                                                .key(),
                                        "64"));
        String[] names = new String[128];
        float[][] vectors = new float[128][2];
        for (int i = 0; i < names.length; i++) {
            names[i] = i > 90 ? "match" : "other";
            vectors[i][0] = i;
        }
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitIdBTreeIndex(table, names.length);
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        // One indexed arm and one unindexed arm make the whole OR unevaluable.
        Predicate filter =
                PredicateBuilder.or(
                        predicates.lessThan(0, 0),
                        predicates.equal(1, BinaryString.fromString("match")));
        for (String factor : Arrays.asList("2", "64")) {
            BatchVectorSearchBuilder builder =
                    table.newBatchVectorSearchBuilder()
                            .withVectors(new float[][] {{0, 0}, {127, 0}})
                            .withVectorColumn("vec")
                            .withLimit(2)
                            .withFilter(filter)
                            .withOption(
                                    CoreOptions.VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR.key(),
                                    factor);
            VectorScan.Plan captured = builder.newVectorScan().scan();
            assertThat(captured.splits()).noneMatch(split -> split instanceof RawVectorSearchSplit);
            List<VectorSearchSplit> transported = new ArrayList<>();
            for (VectorSearchSplit split : captured.splits()) {
                transported.add(
                        InstantiationUtil.deserializeObject(
                                InstantiationUtil.serializeObject(split),
                                getClass().getClassLoader()));
            }
            VectorScan.Plan restored =
                    new VectorScan.Plan() {
                        @Override
                        public List<VectorSearchSplit> splits() {
                            return transported;
                        }

                        @Override
                        public Snapshot snapshot() {
                            return captured.snapshot();
                        }
                    };
            List<GlobalIndexResult> results = builder.newBatchVectorRead().readBatch(restored);
            assertThat(results.get(0).results())
                    .containsExactly(factor.equals("2") ? new Long[0] : new Long[] {91L, 92L});
            assertThat(results.get(1).results()).containsExactlyInAnyOrder(127L, 126L);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"fast", "full", "detail"})
    public void testUnsupportedOrWithPartialScalarCoveragePlansNoRaw(String scalarMode)
            throws Exception {
        FileStoreTable table =
                createTable(
                        "unsupported_partial_or_" + scalarMode,
                        schemaBuilder(true)
                                .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), scalarMode));
        String[] names = {"other", "other", "other", "other", "other", "other", "match", "other"};
        float[][] vectors = linearVectors(names.length);
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitIdBTreeIndex(table, new Range(0, 3));
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate filter =
                PredicateBuilder.or(
                        predicates.lessThan(0, 0),
                        predicates.equal(1, BinaryString.fromString("match")));
        VectorSearchBuilder builder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0, 0})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(filter)
                        .withOption(
                                CoreOptions.VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR.key(), "1");
        VectorScan.Plan plan = builder.newVectorScan().scan();
        assertThat(rawRanges(plan)).isEmpty();
        assertThat(builder.newVectorRead().read(plan).results()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"fast", "full", "detail"})
    public void testPartialScalarAndUsesContributingCoverageAndQueryMode(String scalarMode)
            throws Exception {
        FileStoreTable table =
                createTable(
                        "partial_scalar_and_" + scalarMode,
                        schemaBuilder(true)
                                .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "fast"));
        String[] names = {"other", "match", "other", "other", "other", "other", "match", "other"};
        float[][] vectors = linearVectors(names.length);
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitIdBTreeIndex(table, new Range(0, 3));
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate filter =
                PredicateBuilder.and(
                        predicates.greaterOrEqual(0, 0),
                        predicates.equal(1, BinaryString.fromString("match")));
        VectorSearchBuilder builder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {6, 0})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(filter)
                        .withOption(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), scalarMode);
        VectorScan.Plan plan = transportPlan(builder.newVectorScan().scan());
        if (scalarMode.equals("fast")) {
            assertThat(rawRanges(plan)).isEmpty();
            assertThat(builder.newVectorRead().read(plan).results()).containsExactly(1L);
        } else {
            assertThat(rawRanges(plan)).containsExactly(new Range(4, 7));
            assertThat(builder.newVectorRead().read(plan).results()).containsExactly(6L);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"full", "detail"})
    public void testEmptyScalarMatchesStillPlanUncoveredRows(String scalarMode) throws Exception {
        FileStoreTable table =
                createTable(
                        "empty_partial_scalar_" + scalarMode,
                        schemaBuilder(true)
                                .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "fast"));
        String[] names = {"other", "other", "other", "other", "other", "other", "match", "other"};
        float[][] vectors = linearVectors(names.length);
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitIdBTreeIndex(table, new Range(0, 3));
        Predicate filter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 6);
        VectorSearchBuilder builder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {6, 0})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(filter)
                        .withOption(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), scalarMode);
        VectorScan.Plan plan = builder.newVectorScan().scan();
        assertThat(rawRanges(plan)).containsExactly(new Range(4, 7));
        assertThat(builder.newVectorRead().read(plan).results()).containsExactly(6L);
    }

    @Test
    public void testDeclinedPartialScalarOperatorPlansNoRaw() throws Exception {
        FileStoreTable table =
                createTable(
                        "declined_partial_scalar",
                        schemaBuilder(true)
                                .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "full")
                                .option("btree-index.fallback-scan-max-size", "0"));
        String[] names = {
            "other-0", "other-1", "other-2", "other-3", "other-4", "other-5", "match", "other-7"
        };
        float[][] vectors = linearVectors(names.length);
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names, new Range(0, 3));
        Predicate filter =
                new PredicateBuilder(table.rowType()).contains(1, BinaryString.fromString("match"));
        VectorSearchBuilder builder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0, 0})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(filter)
                        .withOption(
                                CoreOptions.VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR.key(), "1");
        VectorScan.Plan plan = builder.newVectorScan().scan();
        assertThat(rawRanges(plan)).isEmpty();
        assertThat(builder.newVectorRead().read(plan).results()).isEmpty();
    }

    @Test
    public void testExplicitRawSourceRetainsScoringDuringPostFilter() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        String[] names = {"other", "other", "match"};
        float[][] vectors = linearVectors(names.length);
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        Predicate filter =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("match"));
        VectorSearchBuilder builder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0, 0})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(filter)
                        .withOption(
                                CoreOptions.VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR.key(), "1");
        VectorScan.Plan captured = builder.newVectorScan().scan();
        List<VectorSearchSplit> splits = new ArrayList<>(captured.splits());
        splits.add(
                new RawVectorSearchSplit(
                        Collections.singletonList(new Range(2, 2)),
                        Collections.emptyList(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER));
        VectorScan.Plan plan =
                new VectorScan.Plan() {
                    @Override
                    public List<VectorSearchSplit> splits() {
                        return splits;
                    }

                    @Override
                    public Snapshot snapshot() {
                        return captured.snapshot();
                    }
                };
        assertThat(builder.newVectorRead().read(transportPlan(plan)).results()).containsExactly(2L);
    }

    private VectorScan.Plan transportPlan(VectorScan.Plan captured) throws Exception {
        List<VectorSearchSplit> splits = new ArrayList<>();
        for (VectorSearchSplit split : captured.splits()) {
            splits.add(
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(split), getClass().getClassLoader()));
        }
        return new VectorScan.Plan() {
            @Override
            public List<VectorSearchSplit> splits() {
                return splits;
            }

            @Override
            public Snapshot snapshot() {
                return captured.snapshot();
            }
        };
    }

    private static float[][] linearVectors(int count) {
        float[][] vectors = new float[count][2];
        for (int i = 0; i < count; i++) {
            vectors[i][0] = i;
        }
        return vectors;
    }

    private static List<Range> rawRanges(VectorScan.Plan plan) {
        List<Range> ranges = new ArrayList<>();
        for (VectorSearchSplit split : plan.splits()) {
            if (split instanceof RawVectorSearchSplit) {
                ranges.addAll(((RawVectorSearchSplit) split).rowRanges());
            }
        }
        return Range.sortAndMergeOverlap(ranges, true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "bad", "2147483648"})
    public void testPostFilterRejectsInvalidCandidateFactor(String value) throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        assertThatThrownBy(
                        () ->
                                table.newVectorSearchBuilder()
                                        .withVector(new float[] {0, 0})
                                        .withVectorColumn("vec")
                                        .withLimit(2)
                                        .withOption(
                                                CoreOptions
                                                        .VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR
                                                        .key(),
                                                value)
                                        .newVectorRead())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Override
    protected Schema schemaDefault() {
        return schemaBuilder(true).build();
    }

    @Test
    public void testPostFilterPinsCandidateReadsAndDeletionVectorsToPlanSnapshot()
            throws Exception {
        FileStoreTable table =
                createTable(
                        "post_filter_snapshot",
                        schemaBuilder(true)
                                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"));
        String[] names = {"other", "match", "match"};
        float[][] vectors = {{0, 0}, {1, 0}, {2, 0}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        Predicate filter =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("match"));
        VectorSearchBuilder builder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0, 0})
                        .withVectorColumn("vec")
                        .withLimit(1)
                        .withFilter(filter);
        VectorScan.Plan oldPlan = builder.newVectorScan().scan();
        DeletionVectorTestUtils.commitDeletionVectors(table, 1L);
        assertThat(builder.newVectorRead().read(oldPlan).results()).containsExactly(1L);
        assertThat(builder.executeLocal().results()).containsExactly(2L);
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

    @ParameterizedTest
    @CsvSource({"full,true", "detail,true", "full,false", "detail,false"})
    public void testPartialCompositeKeepsScalarFilterCoverage(String mode, boolean indexed)
            throws Exception {
        FileStoreTable table =
                createTable(
                        "vector_composite_" + mode + indexed,
                        schemaBuilder(true)
                                .option(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), mode)
                                .option(CoreOptions.VECTOR_INDEX_SEARCH_MODE.key(), "full"));
        String[] names = {"alpha", "beta", "gamma"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}, {0.0f, 1.0f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors, new Range(0, indexed ? 2 : 0));
        buildAndCommitNameBTreeIndex(table, names);
        buildAndCommitIdBTreeIndex(table, names.length);
        List<DataField> fields =
                Arrays.asList(table.rowType().getField("name"), table.rowType().getField("id"));
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, "btree", fields, table.coreOptions().toConfiguration());
        writer.write(GenericRow.of(BinaryString.fromString(names[0]), 0), 0);
        writer.write(GenericRow.of(BinaryString.fromString(names[1]), 1), 1);
        commitIndex(
                table,
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        new Range(0, 1),
                        fields,
                        "btree",
                        writer.finish(),
                        null));
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate query =
                PredicateBuilder.and(
                        predicates.equal(0, 2),
                        predicates.equal(1, BinaryString.fromString("gamma")));
        assertThat(search(table, query, 1)).containsExactly(2L);
    }

    @Test
    public void testSameFieldResidualBeforeVectorRanking() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        String[] names = {"Alpha", "Beta"};
        float[][] vectors = {{1.0f, 0.0f}, {0.6f, 0.8f}};
        write(table, names, vectors);
        buildAndCommitVectorIndex(table, vectors);
        buildAndCommitNameBTreeIndex(table, names);

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        FieldRef name = new FieldRef(1, "name", table.rowType().getTypeAt(1));
        Predicate filter =
                PredicateBuilder.and(
                        builder.isNotNull(1),
                        builder.equal(
                                new LowerTransform(Collections.singletonList(name)),
                                BinaryString.fromString("beta")));
        assertThat(search(table, filter, 1)).containsExactly(1L);
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
                                Collections.singletonList(vectorField),
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
        buildAndCommitNameBTreeIndex(table, names, new Range(0, names.length - 1));
    }

    private void buildAndCommitNameBTreeIndex(FileStoreTable table, String[] names, Range rowRange)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField nameField = table.rowType().getField("name");
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                BTreeGlobalIndexerFactory.IDENTIFIER,
                                Collections.singletonList(nameField),
                                options);
        // The btree writer needs sorted keys.
        Integer[] order = new Integer[(int) rowRange.count()];
        for (int i = 0; i < order.length; i++) {
            order[i] = (int) rowRange.from + i;
        }
        Arrays.sort(order, (a, b) -> names[a].compareTo(names[b]));
        for (int rowId : order) {
            writer.write(BinaryString.fromString(names[rowId]), rowId - rowRange.from);
        }
        commitIndex(
                table,
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        nameField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        writer.finish()));
    }

    private void buildAndCommitIdBTreeIndex(FileStoreTable table, int rowCount) throws Exception {
        buildAndCommitIdBTreeIndex(table, new Range(0, rowCount - 1));
    }

    private void buildAndCommitIdBTreeIndex(FileStoreTable table, Range rowRange) throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField idField = table.rowType().getField("id");
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                BTreeGlobalIndexerFactory.IDENTIFIER,
                                Collections.singletonList(idField),
                                options);
        for (long rowId = rowRange.from; rowId <= rowRange.to; rowId++) {
            writer.write((int) rowId, rowId - rowRange.from);
        }
        commitIndex(
                table,
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
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
