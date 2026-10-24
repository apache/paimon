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

package org.apache.paimon.spark.read;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.BatchVectorRead;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.PrimaryKeyVectorRead.SearchResult;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SerializableFunction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Spark-dispatched batch vector search. */
public class SparkDataEvolutionBatchVectorReadTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testIndexedBatchDistributesAndKeepsQueryOrder() throws Exception {
        IndexBatchRead read = new IndexBatchRead(createTable(tempDir.resolve("index"), false));

        ScoredGlobalIndexResult[] results = read.readIndexedBatch(indexSplits(4), null);

        assertThat(read.sparkGroups).isEqualTo(2);
        assertThat(results).hasSize(2);
        assertThat(results[0].results()).contains(0L, 1L, 2L, 3L);
        assertThat(results[1].results()).contains(0L, 1L, 2L, 3L);
        assertThat(results[0].scoreGetter().score(0L)).isEqualTo(1.0f);
        assertThat(results[1].scoreGetter().score(0L)).isEqualTo(2.0f);
    }

    @Test
    public void testFewIndexSplitsStayLocal() throws Exception {
        IndexBatchRead read = new IndexBatchRead(createTable(tempDir.resolve("local"), false));

        ScoredGlobalIndexResult[] results = read.readIndexedBatch(indexSplits(1), null);

        assertThat(read.sparkUsed.get()).isFalse();
        assertThat(results).hasSize(2);
        assertThat(results[0].results()).contains(0L);
        assertThat(results[1].scoreGetter().score(0L)).isEqualTo(2.0f);
    }

    @Test
    public void testRawSearchDistributesEachQuery() {
        RawBatchRead read = new RawBatchRead();
        RawVectorSearchSplit rawSplit =
                new RawVectorSearchSplit(
                        Collections.singletonList(new Range(0, 63)), Collections.emptyList(), null);

        List<GlobalIndexResult> results = read.search(Collections.singletonList(rawSplit));

        assertThat(read.sparkGroups).isEqualTo(2);
        assertThat(read.rawSearchRanges)
                .containsExactly(
                        new Range(0, 31), new Range(0, 31), new Range(32, 63), new Range(32, 63));
        assertThat(results).hasSize(2);
        assertThat(results.get(0).results().getLongCardinality()).isEqualTo(64);
        assertThat(results.get(1).results().getLongCardinality()).isEqualTo(64);
        assertThat(((ScoredGlobalIndexResult) results.get(0)).scoreGetter().score(0L))
                .isEqualTo(1.0f);
        assertThat(((ScoredGlobalIndexResult) results.get(1)).scoreGetter().score(0L))
                .isEqualTo(3.0f);
    }

    @Test
    public void testBuilderSelectsDataEvolutionReader() throws Exception {
        FileStoreTable table = createTable(tempDir.resolve("builder"), false);
        BatchVectorRead read =
                new SparkBatchVectorSearchBuilderImpl(table)
                        .withVectorColumn("vec")
                        .withLimit(2)
                        .withVectors(new float[][] {{1.0f}, {2.0f}})
                        .newBatchVectorRead();

        assertThat(read).isInstanceOf(SparkDataEvolutionBatchVectorRead.class);
    }

    @Test
    public void testBuilderSelectsPrimaryKeyReader() throws Exception {
        FileStoreTable table = createTable(tempDir.resolve("pk-builder"), true);
        BatchVectorRead read =
                new SparkBatchVectorSearchBuilderImpl(table)
                        .withVectorColumn("embedding")
                        .withLimit(1)
                        .withVectors(new float[][] {{0.0f, 0.0f}})
                        .newBatchVectorRead();

        assertThat(read).isInstanceOf(SparkPrimaryKeyBatchVectorRead.class);
    }

    @Test
    public void testPrimaryKeyBucketsDistributePerQuery() throws Exception {
        TestingPkBatchRead read =
                new TestingPkBatchRead(createTable(tempDir.resolve("pk-search"), true));
        List<BucketVectorSearchSplit> splits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            splits.add(
                    new BucketVectorSearchSplit(
                            DataSplit.builder()
                                    .withSnapshot(1)
                                    .withPartition(BinaryRow.EMPTY_ROW)
                                    .withBucket(i)
                                    .withBucketPath("bucket-" + i)
                                    .withDataFiles(Collections.emptyList())
                                    .build(),
                            Collections.emptyList()));
        }

        List<SearchResult> results = read.search(splits);

        assertThat(read.sparkGroups).isEqualTo(2);
        assertThat(results).hasSize(2);
    }

    @Test
    public void testDistributedBatchKeepsEveryIndexedField() throws Exception {
        FileStoreTable table = createMultiFieldTable(tempDir.resolve("multi-field"));
        MultiFieldBatchRead read = new MultiFieldBatchRead(table);

        read.readIndexedBatch(multiFieldSplits(table, 4), null);

        assertThat(read.indexerNames)
                .isNotEmpty()
                .allMatch(name -> name.equals("TestMultiFieldVectorGlobalIndexer"));
    }

    @Test
    public void testDistributedBatchReranksWithPersistedMetric() throws Exception {
        FileStoreTable table = createMetricTable(tempDir.resolve("persisted-metric"));
        PersistedMetricBatchRead read = new PersistedMetricBatchRead(table);

        ScoredGlobalIndexResult[] results = read.readIndexedBatch(indexSplits(4), null);

        assertThat(results[0].results()).containsExactly(11L);
        read.combineWithRaw(
                results,
                Collections.singletonList(
                        new RawVectorSearchSplit(
                                Collections.singletonList(new Range(0, 0)),
                                Collections.emptyList(),
                                null)),
                null,
                null);
        assertThat(read.rawMetrics).containsExactly("l2");
    }

    @Test
    public void testDistributedBatchRejectsConflictingPersistedMetrics() throws Exception {
        FileStoreTable table = createMetricTable(tempDir.resolve("conflicting-metric"));
        ConflictingMetricBatchRead read = new ConflictingMetricBatchRead(table);

        assertThatThrownBy(() -> read.readIndexedBatch(indexSplits(4), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different vector metrics")
                .hasMessageContaining("l2")
                .hasMessageContaining("cosine");
    }

    private FileStoreTable createTable(java.nio.file.Path directory, boolean primaryKey)
            throws Exception {
        Path tablePath = new Path(directory.toUri());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.GLOBAL_INDEX_THREAD_NUM, 1);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        Schema.Builder builder = Schema.newBuilder().options(options.toMap());
        if (primaryKey) {
            builder.column("id", DataTypes.INT())
                    .column("embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                    .primaryKey("id")
                    .option(CoreOptions.BUCKET.key(), "1")
                    .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                    .option(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding")
                    .option("fields.embedding.pk-vector.index.type", "test-vector-ann")
                    .option("fields.embedding.pk-vector.distance.metric", "l2");
        } else {
            builder.column("vec", new ArrayType(DataTypes.FLOAT()));
        }
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), tablePath),
                        builder.build());
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    private static List<IndexVectorSearchSplit> indexSplits(int count) {
        List<IndexVectorSearchSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            GlobalIndexMeta globalIndexMeta = new GlobalIndexMeta(i, i, 0, null, new byte[0]);
            IndexFileMeta indexFile =
                    new IndexFileMeta(
                            "test-vector-ann", "index-" + i, 1L, 1L, globalIndexMeta, null);
            splits.add(
                    new IndexVectorSearchSplit(
                            i, i, Collections.singletonList(indexFile), Collections.emptyList()));
        }
        return splits;
    }

    private FileStoreTable createMultiFieldTable(java.nio.file.Path directory) throws Exception {
        return createDataEvolutionTable(
                directory, true, Collections.singletonMap("test.vector.metric", "cosine"));
    }

    private FileStoreTable createMetricTable(java.nio.file.Path directory) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("test.vector.metric", "cosine");
        options.put("refine_factor", "2");
        return createDataEvolutionTable(directory, false, options);
    }

    private FileStoreTable createDataEvolutionTable(
            java.nio.file.Path directory, boolean withLabel, Map<String, String> extraOptions)
            throws Exception {
        Path tablePath = new Path(directory.toUri());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.GLOBAL_INDEX_THREAD_NUM, 1);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        for (Map.Entry<String, String> entry : extraOptions.entrySet()) {
            options.set(entry.getKey(), entry.getValue());
        }
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("vec", new ArrayType(DataTypes.FLOAT()))
                        .options(options.toMap());
        if (withLabel) {
            builder.column("label", DataTypes.INT());
        }
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), tablePath),
                        builder.build());
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    private static List<IndexVectorSearchSplit> multiFieldSplits(FileStoreTable table, int count) {
        DataField vectorField = table.rowType().getField("vec");
        DataField labelField = table.rowType().getField("label");
        int[] extraFieldIds = new int[] {labelField.id()};
        List<IndexVectorSearchSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            GlobalIndexMeta vectorMeta =
                    new GlobalIndexMeta(i, i, vectorField.id(), extraFieldIds, null);
            GlobalIndexMeta scalarMeta =
                    new GlobalIndexMeta(i, i, vectorField.id(), extraFieldIds, new byte[] {1});
            splits.add(
                    new IndexVectorSearchSplit(
                            i,
                            i,
                            Arrays.asList(
                                    new IndexFileMeta(
                                            "test-vector-ann",
                                            "vector-" + i,
                                            1L,
                                            1L,
                                            vectorMeta,
                                            null),
                                    new IndexFileMeta(
                                            "test-vector-ann",
                                            "scalar-" + i,
                                            1L,
                                            1L,
                                            scalarMeta,
                                            null)),
                            Collections.emptyList()));
        }
        return splits;
    }

    private static class IndexBatchRead extends SparkDataEvolutionBatchVectorRead {

        private final AtomicBoolean sparkUsed = new AtomicBoolean(false);
        private int sparkGroups;

        private IndexBatchRead(FileStoreTable table) {
            super(
                    table,
                    null,
                    null,
                    10,
                    table.rowType().getField("vec"),
                    new float[][] {{1.0f}, {2.0f}},
                    null);
        }

        @Override
        protected int sparkParallelism() {
            return 2;
        }

        @Override
        protected List<RoaringNavigableMap64> preFilters(List<IndexVectorSearchSplit> splits) {
            return Collections.emptyList();
        }

        @Override
        protected <I, O> List<O> mapInSpark(
                List<I> data, SerializableFunction<I, O> func, int parallelism) {
            sparkUsed.set(true);
            sparkGroups = data.size();
            return data.stream().map(func::apply).collect(Collectors.toList());
        }

        @Override
        protected CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> evalBatch(
                GlobalIndexer globalIndexer,
                IndexPathFactory indexPathFactory,
                long rowRangeStart,
                long rowRangeEnd,
                List<IndexFileMeta> vectorIndexFiles,
                float[][] vectors,
                int searchLimit,
                @Nullable RoaringNavigableMap64 includeRowIds,
                ExecutorService executor) {
            List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(vectors.length);
            for (float[] vector : vectors) {
                RoaringNavigableMap64 rows = new RoaringNavigableMap64();
                rows.add(rowRangeStart);
                float score = vector[0];
                results.add(Optional.of(ScoredGlobalIndexResult.create(rows, rowId -> score)));
            }
            return CompletableFuture.completedFuture(results);
        }
    }

    private static class RawBatchRead extends SparkDataEvolutionBatchVectorRead {

        private final List<Range> rawSearchRanges = new ArrayList<>();
        private int sparkGroups;

        private RawBatchRead() {
            super(
                    null,
                    null,
                    null,
                    100,
                    new org.apache.paimon.types.DataField(
                            0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[][] {{1.0f}, {3.0f}},
                    Collections.singletonMap("test.vector.metric", "l2"));
        }

        private List<GlobalIndexResult> search(List<RawVectorSearchSplit> rawSplits) {
            return combineWithRaw(emptyScoredResults(2), rawSplits, null, null);
        }

        @Override
        protected int sparkParallelism() {
            return 2;
        }

        @Override
        protected <I, O> List<O> mapInSpark(
                List<I> data, SerializableFunction<I, O> func, int parallelism) {
            sparkGroups = data.size();
            return data.stream().map(func::apply).collect(Collectors.toList());
        }

        @Override
        protected ScoredGlobalIndexResult readRawSearch(
                List<Range> rawRowRanges,
                @Nullable RoaringNavigableMap64 preFilter,
                String metric,
                float[] queryVector) {
            assertThat(metric).isEqualTo("l2");
            rawSearchRanges.addAll(rawRowRanges);
            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            for (Range range : rawRowRanges) {
                rows.addRange(range);
            }
            float score = queryVector[0];
            return ScoredGlobalIndexResult.create(rows, rowId -> score);
        }
    }

    private static class TestingPkBatchRead extends SparkPrimaryKeyBatchVectorRead {

        private int sparkGroups;

        private TestingPkBatchRead(FileStoreTable table) {
            super(
                    table,
                    table.rowType().getField("embedding"),
                    new float[][] {{0.0f, 0.0f}, {1.0f, 0.0f}},
                    1,
                    Collections.emptyMap(),
                    null);
        }

        private List<SearchResult> search(List<BucketVectorSearchSplit> splits) {
            return searchAllBuckets(queryReads.get(0), splits);
        }

        @Override
        protected int sparkParallelism() {
            return 2;
        }

        @Override
        protected <I, O> List<O> mapInSpark(
                List<I> data, SerializableFunction<I, O> func, int parallelism) {
            sparkGroups = data.size();
            return data.stream().map(func::apply).collect(Collectors.toList());
        }

        @Override
        protected List<SearchResult> searchBucketGroup(
                org.apache.paimon.table.source.PrimaryKeyVectorRead firstRead,
                List<BucketVectorSearchSplit> splits) {
            List<SearchResult> results = new ArrayList<>(queries.length);
            for (int i = 0; i < queries.length; i++) {
                results.add(new SearchResult(Collections.emptyList(), Collections.emptyList()));
            }
            return results;
        }
    }

    private static class MultiFieldBatchRead extends SparkDataEvolutionBatchVectorRead {

        private final List<String> indexerNames = new ArrayList<>();

        private MultiFieldBatchRead(FileStoreTable table) {
            super(
                    table,
                    null,
                    null,
                    1,
                    table.rowType().getField("vec"),
                    new float[][] {{1.0f, 0.0f}},
                    null);
        }

        @Override
        protected int sparkParallelism() {
            return 2;
        }

        @Override
        protected List<RoaringNavigableMap64> preFilters(List<IndexVectorSearchSplit> splits) {
            return Collections.emptyList();
        }

        @Override
        protected <I, O> List<O> mapInSpark(
                List<I> data, SerializableFunction<I, O> func, int parallelism) {
            return data.stream().map(func::apply).collect(Collectors.toList());
        }

        @Override
        protected CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> evalBatch(
                GlobalIndexer globalIndexer,
                IndexPathFactory indexPathFactory,
                long rowRangeStart,
                long rowRangeEnd,
                List<IndexFileMeta> vectorIndexFiles,
                float[][] vectors,
                int searchLimit,
                @Nullable RoaringNavigableMap64 includeRowIds,
                ExecutorService executor) {
            indexerNames.add(globalIndexer.getClass().getSimpleName());
            return emptyHits(vectors.length, rowRangeStart);
        }
    }

    private static class PersistedMetricBatchRead extends SparkDataEvolutionBatchVectorRead {

        private final List<String> rawMetrics = new ArrayList<>();

        private PersistedMetricBatchRead(FileStoreTable table) {
            super(
                    table,
                    null,
                    null,
                    1,
                    table.rowType().getField("vec"),
                    new float[][] {{1.0f, 0.0f}},
                    Collections.singletonMap("refine_factor", "2"));
        }

        @Override
        protected int sparkParallelism() {
            return 2;
        }

        @Override
        protected List<RoaringNavigableMap64> preFilters(List<IndexVectorSearchSplit> splits) {
            return Collections.emptyList();
        }

        @Override
        protected GlobalIndexer createGlobalIndexer(List<IndexVectorSearchSplit> splits) {
            return new PersistingMetricIndexer("cosine", "l2");
        }

        @Override
        protected <I, O> List<O> mapInSpark(
                List<I> data, SerializableFunction<I, O> func, int parallelism) {
            return data.stream().map(func::apply).collect(Collectors.toList());
        }

        @Override
        protected CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> evalBatch(
                GlobalIndexer globalIndexer,
                IndexPathFactory indexPathFactory,
                long rowRangeStart,
                long rowRangeEnd,
                List<IndexFileMeta> vectorIndexFiles,
                float[][] vectors,
                int searchLimit,
                @Nullable RoaringNavigableMap64 includeRowIds,
                ExecutorService executor) {
            globalIndexer.createReader(null, Collections.emptyList(), 1, null, executor);
            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            rows.add(10L);
            rows.add(11L);
            List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(vectors.length);
            for (int i = 0; i < vectors.length; i++) {
                results.add(Optional.of(ScoredGlobalIndexResult.create(rows, rowId -> 1.0f)));
            }
            return CompletableFuture.completedFuture(results);
        }

        @Override
        protected Map<Long, float[]> readRawVectors(
                RoaringNavigableMap64 candidates, boolean includeFilter) {
            Map<Long, float[]> vectors = new HashMap<>();
            vectors.put(10L, new float[] {2.0f, 0.0f});
            vectors.put(11L, new float[] {1.0f, 0.1f});
            return vectors;
        }

        @Override
        protected ScoredGlobalIndexResult readRawSearch(
                List<Range> rawRowRanges,
                @Nullable RoaringNavigableMap64 preFilter,
                String metric,
                float[] queryVector) {
            rawMetrics.add(metric);
            return ScoredGlobalIndexResult.createEmpty();
        }
    }

    private static class ConflictingMetricBatchRead extends SparkDataEvolutionBatchVectorRead {

        private ConflictingMetricBatchRead(FileStoreTable table) {
            super(
                    table,
                    null,
                    null,
                    1,
                    table.rowType().getField("vec"),
                    new float[][] {{1.0f, 0.0f}},
                    null);
        }

        @Override
        protected int sparkParallelism() {
            return 2;
        }

        @Override
        protected List<RoaringNavigableMap64> preFilters(List<IndexVectorSearchSplit> splits) {
            return Collections.emptyList();
        }

        @Override
        protected GlobalIndexer createGlobalIndexer(List<IndexVectorSearchSplit> splits) {
            String persisted = splits.get(0).rowRangeStart() < 2 ? "l2" : "cosine";
            return new PersistingMetricIndexer("cosine", persisted);
        }

        @Override
        protected <I, O> List<O> mapInSpark(
                List<I> data, SerializableFunction<I, O> func, int parallelism) {
            return data.stream().map(func::apply).collect(Collectors.toList());
        }

        @Override
        protected CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> evalBatch(
                GlobalIndexer globalIndexer,
                IndexPathFactory indexPathFactory,
                long rowRangeStart,
                long rowRangeEnd,
                List<IndexFileMeta> vectorIndexFiles,
                float[][] vectors,
                int searchLimit,
                @Nullable RoaringNavigableMap64 includeRowIds,
                ExecutorService executor) {
            globalIndexer.createReader(null, Collections.emptyList(), 1, null, executor);
            return emptyHits(vectors.length, rowRangeStart);
        }
    }

    private static CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> emptyHits(
            int queryCount, long rowId) {
        List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(queryCount);
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        rows.add(rowId);
        for (int i = 0; i < queryCount; i++) {
            results.add(Optional.of(ScoredGlobalIndexResult.create(rows, ignored -> 1.0f)));
        }
        return CompletableFuture.completedFuture(results);
    }

    private static final class PersistingMetricIndexer implements VectorGlobalIndexer {

        private final String configuredMetric;
        private final String persistedMetric;
        private boolean opened;

        private PersistingMetricIndexer(String configuredMetric, String persistedMetric) {
            this.configuredMetric = configuredMetric;
            this.persistedMetric = persistedMetric;
        }

        @Override
        public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public GlobalIndexReader createReader(
                GlobalIndexFileReader fileReader,
                List<GlobalIndexIOMeta> files,
                long totalRowCount,
                List<Range> rowRanges,
                ExecutorService executor) {
            opened = true;
            return null;
        }

        @Override
        public String metric() {
            return opened ? persistedMetric : configuredMetric;
        }
    }
}
