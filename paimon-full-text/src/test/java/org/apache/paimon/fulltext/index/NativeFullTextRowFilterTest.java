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

package org.apache.paimon.fulltext.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ENABLED;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Row filters on full-text search against the native engine: correctness of filter-then-rank, and a
 * manual benchmark comparing the index pre-filter with the over-fetch workaround.
 */
public class NativeFullTextRowFilterTest {

    private static final String[] WORDS = {
        "paimon", "lake", "stream", "batch", "index", "vector", "search", "table", "commit",
        "snapshot", "manifest", "bucket", "partition", "schema", "column", "row", "query", "scan",
        "read", "write"
    };

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testFilterThenRankMatchesFilteredUnfilteredRanking() throws Exception {
        int rowCount = 2_000;
        int categories = 8;
        Dataset dataset = writeDataset("filter_rank", rowCount, categories, 42);
        FileStoreTable table = dataset.table;
        PredicateBuilder builder = new PredicateBuilder(table.rowType());

        String query = matchQuery("paimon lake");
        int limit = 20;

        // Reference: rank every row without a filter, then keep the wanted category. BM25 scores
        // do not depend on the filter, so filter-then-rank must equal rank-then-filter.
        ScoredGlobalIndexResult all =
                (ScoredGlobalIndexResult)
                        table.newFullTextSearchBuilder()
                                .withQuery("content", query)
                                .withLimit(rowCount)
                                .executeLocal();
        for (int category = 0; category < categories; category++) {
            List<Long> expected = new ArrayList<>();
            for (long rowId : all.results()) {
                if (dataset.categoryOf.get(rowId) == category) {
                    expected.add(rowId);
                }
            }
            expected.sort(
                    (a, b) -> {
                        int byScore =
                                Float.compare(
                                        all.scoreGetter().score(b), all.scoreGetter().score(a));
                        return byScore != 0 ? byScore : Long.compare(a, b);
                    });
            List<Long> expectedTopK = expected.subList(0, Math.min(limit, expected.size()));
            float cutoff =
                    expectedTopK.isEmpty()
                            ? 0f
                            : all.scoreGetter().score(expectedTopK.get(expectedTopK.size() - 1));

            ScoredGlobalIndexResult filtered =
                    (ScoredGlobalIndexResult)
                            table.newFullTextSearchBuilder()
                                    .withQuery("content", query)
                                    .withLimit(limit)
                                    .withFilter(builder.equal(1, category))
                                    .executeLocal();

            assertThat(filtered.results().getLongCardinality()).isEqualTo(expectedTopK.size());
            for (long rowId : filtered.results()) {
                assertThat(dataset.categoryOf.get(rowId)).isEqualTo(category);
                assertThat(all.results().contains(rowId)).isTrue();
                // Every returned row scores at least as high as the k-th expected row; ties at the
                // cutoff may be broken differently by the engine.
                assertThat(filtered.scoreGetter().score(rowId)).isGreaterThanOrEqualTo(cutoff);
                assertThat(filtered.scoreGetter().score(rowId))
                        .isEqualTo(all.scoreGetter().score(rowId));
            }
        }

        // A filter that matches nothing returns nothing.
        GlobalIndexResult none =
                table.newFullTextSearchBuilder()
                        .withQuery("content", query)
                        .withLimit(limit)
                        .withFilter(builder.equal(1, categories + 1))
                        .executeLocal();
        assertThat(none.results().isEmpty()).isTrue();
    }

    @Test
    public void testUnindexedFilterInFullModeKeepsOriginalRanking() throws Exception {
        // `id` has no scalar index. In scalar-index.search-mode=full the filter must be resolved
        // against the existing full-text index, not by rebuilding an index over the filtered
        // rows: a smaller corpus changes BM25 document frequencies and average length, which
        // changes which rows make the top-k, not just their reported scores.
        int rowCount = 2_000;
        Dataset dataset = writeDataset("unindexed_full_mode", rowCount, 8, 42);
        FileStoreTable table =
                (FileStoreTable)
                        dataset.table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "full"));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        String query = matchQuery("paimon lake");
        int limit = 20;
        int idFrom = 1_800;

        ScoredGlobalIndexResult all =
                (ScoredGlobalIndexResult)
                        table.newFullTextSearchBuilder()
                                .withQuery("content", query)
                                .withLimit(rowCount)
                                .executeLocal();
        List<Long> expected = new ArrayList<>();
        for (long rowId : all.results()) {
            if (rowId >= idFrom) {
                expected.add(rowId);
            }
        }
        expected.sort(
                (a, b) -> {
                    int byScore =
                            Float.compare(all.scoreGetter().score(b), all.scoreGetter().score(a));
                    return byScore != 0 ? byScore : Long.compare(a, b);
                });
        List<Long> expectedTopK = expected.subList(0, Math.min(limit, expected.size()));
        float cutoff = all.scoreGetter().score(expectedTopK.get(expectedTopK.size() - 1));

        ScoredGlobalIndexResult filtered =
                (ScoredGlobalIndexResult)
                        table.newFullTextSearchBuilder()
                                .withQuery("content", query)
                                .withLimit(limit)
                                .withFilter(builder.greaterOrEqual(0, idFrom))
                                .executeLocal();

        List<Long> actual = new ArrayList<>();
        filtered.results().forEach(actual::add);
        // Membership first: every returned row must reach the cutoff of the original ranking.
        for (long rowId : actual) {
            assertThat(rowId).isGreaterThanOrEqualTo(idFrom);
            assertThat(all.scoreGetter().score(rowId))
                    .as(
                            "row %s (original score %s) is below the filtered top-%s cutoff %s",
                            rowId, all.scoreGetter().score(rowId), limit, cutoff)
                    .isGreaterThanOrEqualTo(cutoff);
        }
        assertThat(actual).hasSize(expectedTopK.size());
        for (long rowId : actual) {
            assertThat(filtered.scoreGetter().score(rowId))
                    .as("row %s must keep its score from the original index", rowId)
                    .isEqualTo(all.scoreGetter().score(rowId));
        }
    }

    @Test
    public void testRawPathAppliesFilterOverTheWholeRawCorpus() throws Exception {
        // Rows 0-999 are covered by the full-text index, rows 1000-1999 are not. In full-text
        // mode full the uncovered rows are searched through a temporary index; a filter must not
        // shrink that temporary corpus, so scores of the returned raw rows equal the scores of an
        // unfiltered raw search.
        int rowCount = 2_000;
        int indexedRows = 1_000;
        Dataset dataset = writeDataset("raw_corpus", rowCount, 8, 42, indexedRows);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FULL_TEXT_INDEX_SEARCH_MODE.key(), "full");
        options.put(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "full");
        FileStoreTable table = (FileStoreTable) dataset.table.copy(options);
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        String query = matchQuery("paimon lake");
        int limit = 20;
        int idFrom = 1_800;

        ScoredGlobalIndexResult unfiltered =
                (ScoredGlobalIndexResult)
                        table.newFullTextSearchBuilder()
                                .withQuery("content", query)
                                .withLimit(rowCount)
                                .executeLocal();
        ScoredGlobalIndexResult filtered =
                (ScoredGlobalIndexResult)
                        table.newFullTextSearchBuilder()
                                .withQuery("content", query)
                                .withLimit(limit)
                                .withFilter(builder.greaterOrEqual(0, idFrom))
                                .executeLocal();

        assertThat(filtered.results().getLongCardinality()).isEqualTo(limit);
        for (long rowId : filtered.results()) {
            assertThat(rowId).isGreaterThanOrEqualTo(idFrom);
            assertThat(unfiltered.results().contains(rowId)).isTrue();
            assertThat(filtered.scoreGetter().score(rowId))
                    .as("row %s must keep the score of the unfiltered raw corpus", rowId)
                    .isEqualTo(unfiltered.scoreGetter().score(rowId));
        }
    }

    /**
     * Run with {@code mvn -pl paimon-full-text test -Dtest=NativeFullTextRowFilterTest
     * -DextraJavaTestArgs=-Dpaimon.benchmark=true}. Prints one line per strategy.
     */
    @Test
    @EnabledIfSystemProperty(named = "paimon.benchmark", matches = "true")
    public void benchmarkRowFilterStrategies() throws Exception {
        int rowCount = 200_000;
        int categories = 100; // 1% selectivity per category
        Dataset dataset = writeDataset("benchmark", rowCount, categories, 7);
        FileStoreTable table = dataset.table;
        FileStoreTable fullModeTable =
                (FileStoreTable)
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "full"));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        String query = matchQuery("paimon lake");
        int limit = 10;
        int category = 17;

        System.out.printf(
                "%nfull-text row filter benchmark: rows=%d, categories=%d, limit=%d%n",
                rowCount, categories, limit);
        long baseline =
                time(
                        "no filter (baseline)",
                        () ->
                                table.newFullTextSearchBuilder()
                                        .withQuery("content", query)
                                        .withLimit(limit)
                                        .executeLocal());
        long preFilter =
                time(
                        "withFilter via btree index (1% selective)",
                        () ->
                                table.newFullTextSearchBuilder()
                                        .withQuery("content", query)
                                        .withLimit(limit)
                                        .withFilter(builder.equal(1, category))
                                        .executeLocal());
        time(
                "withFilter via btree index (dense, ~99% selective)",
                () ->
                        table.newFullTextSearchBuilder()
                                .withQuery("content", query)
                                .withLimit(limit)
                                .withFilter(builder.notEqual(1, category))
                                .executeLocal());
        time(
                "over-fetch limit*100 then client-side filter (old workaround)",
                () -> {
                    ScoredGlobalIndexResult result =
                            (ScoredGlobalIndexResult)
                                    table.newFullTextSearchBuilder()
                                            .withQuery("content", query)
                                            .withLimit(limit * 100)
                                            .executeLocal();
                    List<Long> kept = new ArrayList<>();
                    for (long rowId : result.results()) {
                        if (dataset.categoryOf.get(rowId) == category) {
                            kept.add(rowId);
                        }
                    }
                    return kept;
                });
        Predicate unindexedFilter = builder.equal(2, 3);
        time(
                "withFilter on unindexed column, scalar mode full (raw scan)",
                () ->
                        fullModeTable
                                .newFullTextSearchBuilder()
                                .withQuery("content", query)
                                .withLimit(limit)
                                .withFilter(unindexedFilter)
                                .executeLocal());
        System.out.printf(
                "pre-filter overhead over baseline: %.1fx%n", (double) preFilter / baseline);
    }

    private static long time(String name, Supplier<Object> action) {
        for (int i = 0; i < 3; i++) {
            action.get();
        }
        int iterations = 10;
        long best = Long.MAX_VALUE;
        long total = 0;
        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            action.get();
            long elapsed = System.nanoTime() - start;
            best = Math.min(best, elapsed);
            total += elapsed;
        }
        System.out.printf(
                "  %-64s best %7.1f ms  avg %7.1f ms%n",
                name, best / 1_000_000.0, total / iterations / 1_000_000.0);
        return best;
    }

    private static final class Dataset {
        final FileStoreTable table;
        final Map<Long, Integer> categoryOf;

        Dataset(FileStoreTable table, Map<Long, Integer> categoryOf) {
            this.table = table;
            this.categoryOf = categoryOf;
        }
    }

    /**
     * Table (id INT, category INT, other INT, content STRING) with a native full-text index on
     * {@code content} and a btree index on {@code category}; {@code other} stays unindexed.
     */
    private Dataset writeDataset(String tableName, int rowCount, int categories, long seed)
            throws Exception {
        return writeDataset(tableName, rowCount, categories, seed, rowCount);
    }

    private Dataset writeDataset(
            String tableName, int rowCount, int categories, long seed, int fullTextIndexedRows)
            throws Exception {
        Path tablePath = new Path(tempDir.resolve(tableName).toUri());
        LocalFileIO fileIO = LocalFileIO.create();

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()
                        },
                        new String[] {"id", "category", "other", "content"});
        Options options = new Options();
        options.set(PATH, tablePath.toString());
        options.set(ROW_TRACKING_ENABLED, true);
        options.set(DATA_EVOLUTION_ENABLED, true);
        options.set(GLOBAL_INDEX_ENABLED, true);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                new AppendOnlyFileStoreTable(
                        FileIOFinder.find(tablePath),
                        tablePath,
                        tableSchema,
                        CatalogEnvironment.empty());

        Random random = new Random(seed);
        Map<Long, Integer> categoryOf = new HashMap<>();
        List<String> contents = new ArrayList<>(rowCount);
        List<Integer> categoryValues = new ArrayList<>(rowCount);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < rowCount; i++) {
                int category = random.nextInt(categories);
                StringBuilder content = new StringBuilder();
                int words = 3 + random.nextInt(8);
                for (int w = 0; w < words; w++) {
                    content.append(WORDS[random.nextInt(WORDS.length)]).append(' ');
                }
                contents.add(content.toString().trim());
                categoryValues.add(category);
                categoryOf.put((long) i, category);
                write.write(
                        GenericRow.of(
                                i,
                                category,
                                random.nextInt(10),
                                BinaryString.fromString(contents.get(i))));
            }
            commit.commit(write.prepareCommit());
        }

        Range rowRange = new Range(0, rowCount - 1);
        DataField contentField = table.rowType().getField("content");
        GlobalIndexSingleColumnWriter textWriter =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                NativeFullTextGlobalIndexerFactory.IDENTIFIER,
                                contentField,
                                table.coreOptions().toConfiguration());
        for (int i = 0; i < fullTextIndexedRows; i++) {
            textWriter.write(BinaryString.fromString(contents.get(i)), i);
        }
        List<IndexFileMeta> indexFiles =
                new ArrayList<>(
                        GlobalIndexBuilderUtils.toIndexFileMetas(
                                table.fileIO(),
                                table.store().pathFactory().globalIndexFileFactory(),
                                table.coreOptions(),
                                new Range(0, fullTextIndexedRows - 1),
                                contentField.id(),
                                NativeFullTextGlobalIndexerFactory.IDENTIFIER,
                                textWriter.finish()));

        DataField categoryField = table.rowType().getField("category");
        GlobalIndexSingleColumnWriter categoryWriter =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                BTreeGlobalIndexerFactory.IDENTIFIER,
                                categoryField,
                                table.coreOptions().toConfiguration());
        // The btree writer is an SST writer: keys must arrive in sorted order.
        List<Integer> rowIdsByCategory = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rowIdsByCategory.add(i);
        }
        rowIdsByCategory.sort(
                (a, b) -> {
                    int byCategory = Integer.compare(categoryValues.get(a), categoryValues.get(b));
                    return byCategory != 0 ? byCategory : Integer.compare(a, b);
                });
        for (int rowId : rowIdsByCategory) {
            categoryWriter.write(categoryValues.get(rowId), rowId);
        }
        List<ResultEntry> categoryEntries = categoryWriter.finish();
        indexFiles.addAll(
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        categoryField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        categoryEntries));

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
        return new Dataset(table, categoryOf);
    }

    private static String matchQuery(String terms) {
        return "{\"match\":{\"query\":\"" + terms + "\"}}";
    }
}
