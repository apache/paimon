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

package org.apache.paimon.elasticsearch.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.lumina.index.LuminaVectorGlobalIndexReader;
import org.apache.paimon.lumina.index.LuminaVectorGlobalIndexerFactory;
import org.apache.paimon.lumina.index.LuminaVectorIndexOptions;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.VectorSearch;
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
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

import static org.apache.paimon.table.source.snapshot.TimeTravelUtil.tryTravelOrLatest;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Benchmark for Lumina DiskANN vector search using lower-level APIs:
 *
 * <ul>
 *   <li>Scenario A: Pure vector search via {@link LuminaVectorGlobalIndexReader}
 *   <li>Scenario B: Scalar BTree filter (high selectivity ~20%) → rowIds → vector search
 *   <li>Scenario C: Scalar BTree filter (low selectivity ~50%) → rowIds → vector search
 * </ul>
 *
 * <p>Scalar+Vector flow: uses {@link GlobalIndexScanner#scan(Predicate)} to obtain filtered rowIds,
 * then passes them via {@link VectorSearch#withIncludeRowIds} to {@link
 * LuminaVectorGlobalIndexReader#visitVectorSearch}.
 *
 * <p>Excluded from normal test runs by surefire's {@code **\/*Benchmark*} pattern. Requires Lumina
 * native library; auto-skips if unavailable.
 *
 * <p>Run:
 *
 * <pre>{@code
 * mvn test -pl paimon-elasticsearch -Dtest=LuceneVsESBenchmark#benchmark
 * }</pre>
 */
public class LuceneVsESBenchmark extends TableTestBase {

    private static final String VEC_FIELD = "vec";
    private static final int TOP_K = 10;
    private static final int WARMUP = 5;
    private static final int ITERATIONS = 50;
    private static final long SEED = 42L;

    @Override
    protected Schema schemaDefault() {
        return schemaForDim(32);
    }

    private Schema schemaForDim(int dim) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("category", DataTypes.INT())
                .column(VEC_FIELD, new ArrayType(DataTypes.FLOAT()))
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option("lumina.index.dimension", String.valueOf(dim))
                .option("lumina.distance.metric", "l2")
                .option("lumina.encoding.type", "rawf32")
                .build();
    }

    @Test
    public void benchmark() throws Exception {
        ensureLumina();

        int[] rowCounts = {1000, 5000, 10000};
        int[] dimensions = {32, 128};

        System.out.println("============================================================");
        System.out.println("  Lumina DiskANN: Pure Vector vs Scalar+Vector Benchmark");
        System.out.println("  (Lower-level API: GlobalIndexScanner + LuminaReader)");
        System.out.println("============================================================");
        System.out.printf("TopK: %d  Warmup: %d  Iterations: %d%n%n", TOP_K, WARMUP, ITERATIONS);

        for (int dim : dimensions) {
            for (int numRows : rowCounts) {
                runOneCfg(numRows, dim);
                System.out.println();

                dropTableDefault();
            }
        }
    }

    private void runOneCfg(int numRows, int dim) throws Exception {
        catalog.createTable(identifier(), schemaForDim(dim), true);
        FileStoreTable table = getTableDefault();

        SplittableRandom rng = new SplittableRandom(SEED);
        float[][] vectors = new float[numRows][dim];
        int[] ids = new int[numRows];
        int[] categories = new int[numRows];
        for (int i = 0; i < numRows; i++) {
            ids[i] = i;
            categories[i] = i % 5;
            for (int d = 0; d < dim; d++) {
                vectors[i][d] = (float) rng.nextDouble() * 2 - 1;
            }
        }

        writeData(table, vectors, ids, categories);

        Range fullRange = new Range(0, numRows - 1);
        buildAndCommitVectorIndex(table, vectors, fullRange);
        buildAndCommitBTreeIndex(table, "id", ids, fullRange);

        FileStoreTable reloaded = getTableDefault();

        // Collect committed index files by type
        IndexPathFactory indexPathFactory = reloaded.store().pathFactory().globalIndexFileFactory();
        List<IndexFileMeta> allIndexFiles = getCommittedIndexFiles(reloaded);
        List<IndexFileMeta> vectorIndexFiles =
                allIndexFiles.stream()
                        .filter(
                                f ->
                                        f.indexType()
                                                .equals(
                                                        LuminaVectorGlobalIndexerFactory
                                                                .IDENTIFIER))
                        .collect(Collectors.toList());
        List<IndexFileMeta> btreeIndexFiles =
                allIndexFiles.stream()
                        .filter(f -> f.indexType().equals(BTreeGlobalIndexerFactory.IDENTIFIER))
                        .collect(Collectors.toList());

        // Build Lumina reader from committed vector index files
        List<GlobalIndexIOMeta> vectorIOMetas =
                vectorIndexFiles.stream()
                        .map(meta -> toGlobalIOMeta(meta, indexPathFactory))
                        .collect(Collectors.toList());
        GlobalIndexFileReader fileReader =
                meta -> reloaded.fileIO().newInputStream(meta.filePath());
        LuminaVectorIndexOptions luminaOptions =
                new LuminaVectorIndexOptions(reloaded.coreOptions().toConfiguration());

        float[] query = new float[dim];
        SplittableRandom qrng = new SplittableRandom(99999L);
        for (int d = 0; d < dim; d++) {
            query[d] = (float) qrng.nextDouble() * 2 - 1;
        }

        int filterHigh = (int) (numRows * 0.8);
        int filterLow = numRows / 2;

        // Warmup
        for (int i = 0; i < WARMUP; i++) {
            try (LuminaVectorGlobalIndexReader reader =
                    new LuminaVectorGlobalIndexReader(
                            fileReader,
                            vectorIOMetas,
                            new ArrayType(DataTypes.FLOAT()),
                            luminaOptions)) {
                searchPureVector(reader, query);
            }
            searchWithScalarFilter(
                    reloaded,
                    btreeIndexFiles,
                    fileReader,
                    vectorIOMetas,
                    luminaOptions,
                    query,
                    filterHigh);
            searchWithScalarFilter(
                    reloaded,
                    btreeIndexFiles,
                    fileReader,
                    vectorIOMetas,
                    luminaOptions,
                    query,
                    filterLow);
        }

        // Scenario A: Pure vector search
        long[] latA = new long[ITERATIONS];
        try (LuminaVectorGlobalIndexReader reader =
                new LuminaVectorGlobalIndexReader(
                        fileReader,
                        vectorIOMetas,
                        new ArrayType(DataTypes.FLOAT()),
                        luminaOptions)) {
            for (int i = 0; i < ITERATIONS; i++) {
                long s = System.nanoTime();
                searchPureVector(reader, query);
                latA[i] = System.nanoTime() - s;
            }
        }

        // Scenario B: Scalar filter high selectivity (~20% pass)
        long[] latB = new long[ITERATIONS];
        ScoredGlobalIndexResult lastB = null;
        for (int i = 0; i < ITERATIONS; i++) {
            long s = System.nanoTime();
            lastB =
                    searchWithScalarFilter(
                            reloaded,
                            btreeIndexFiles,
                            fileReader,
                            vectorIOMetas,
                            luminaOptions,
                            query,
                            filterHigh);
            latB[i] = System.nanoTime() - s;
        }

        // Scenario C: Scalar filter low selectivity (~50% pass)
        long[] latC = new long[ITERATIONS];
        ScoredGlobalIndexResult lastC = null;
        for (int i = 0; i < ITERATIONS; i++) {
            long s = System.nanoTime();
            lastC =
                    searchWithScalarFilter(
                            reloaded,
                            btreeIndexFiles,
                            fileReader,
                            vectorIOMetas,
                            luminaOptions,
                            query,
                            filterLow);
            latC[i] = System.nanoTime() - s;
        }

        if (lastB != null) {
            verifyFilter(lastB, filterHigh);
        }
        if (lastC != null) {
            verifyFilter(lastC, filterLow);
        }

        printReport(numRows, dim, latA, latB, latC, filterHigh, filterLow);
    }

    // ====================== Search ======================

    private ScoredGlobalIndexResult searchPureVector(
            LuminaVectorGlobalIndexReader reader, float[] query) {
        VectorSearch vs = new VectorSearch(query, TOP_K, VEC_FIELD);
        Optional<ScoredGlobalIndexResult> result = reader.visitVectorSearch(vs);
        return result.orElse(null);
    }

    private ScoredGlobalIndexResult searchWithScalarFilter(
            FileStoreTable table,
            List<IndexFileMeta> btreeIndexFiles,
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> vectorIOMetas,
            LuminaVectorIndexOptions luminaOptions,
            float[] query,
            int minId)
            throws IOException {
        // Step 1: Scalar scan via GlobalIndexScanner → get filtered rowIds
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        Predicate scalarPredicate = pb.greaterOrEqual(0, minId);

        RoaringNavigableMap64 rowIds;
        try (GlobalIndexScanner scanner =
                GlobalIndexScanner.create(table, btreeIndexFiles).orElse(null)) {
            if (scanner == null) {
                return null;
            }
            Optional<GlobalIndexResult> scalarResult = scanner.scan(scalarPredicate);
            if (!scalarResult.isPresent()) {
                return null;
            }
            rowIds = scalarResult.get().results();
        }

        // Step 2: Vector search with pre-filtered rowIds via LuminaVectorGlobalIndexReader
        try (LuminaVectorGlobalIndexReader reader =
                new LuminaVectorGlobalIndexReader(
                        fileReader,
                        vectorIOMetas,
                        new ArrayType(DataTypes.FLOAT()),
                        luminaOptions)) {
            VectorSearch vs = new VectorSearch(query, TOP_K, VEC_FIELD).withIncludeRowIds(rowIds);
            return reader.visitVectorSearch(vs).orElse(null);
        }
    }

    // ====================== Index File Helpers ======================

    private List<IndexFileMeta> getCommittedIndexFiles(FileStoreTable table) {
        return table.store().newIndexFileHandler()
                .scan(
                        tryTravelOrLatest(table),
                        entry -> entry.indexFile().globalIndexMeta() != null)
                .stream()
                .map(IndexManifestEntry::indexFile)
                .collect(Collectors.toList());
    }

    private GlobalIndexIOMeta toGlobalIOMeta(IndexFileMeta meta, IndexPathFactory pathFactory) {
        GlobalIndexMeta globalIndex = checkNotNull(meta.globalIndexMeta());
        return new GlobalIndexIOMeta(
                pathFactory.toPath(meta), meta.fileSize(), globalIndex.indexMeta());
    }

    // ====================== Verify ======================

    private void verifyFilter(ScoredGlobalIndexResult result, int minId) {
        RoaringNavigableMap64 rows = result.results();
        for (long rowId : rows) {
            if (rowId < minId) {
                throw new AssertionError(
                        "Filtered result contains rowId " + rowId + " < minId " + minId);
            }
        }
    }

    // ====================== Report ======================

    private void printReport(
            int numRows, int dim, long[] latA, long[] latB, long[] latC, int minIdB, int minIdC) {
        Arrays.sort(latA);
        Arrays.sort(latB);
        Arrays.sort(latC);

        int selB = (numRows - minIdB) * 100 / numRows;
        int selC = (numRows - minIdC) * 100 / numRows;

        System.out.printf("--- %,d rows x %d dim ---%n", numRows, dim);
        System.out.printf(
                "%-36s %12s %12s %12s%n",
                "", "Pure Vector", "Filter~" + selB + "%", "Filter~" + selC + "%");
        System.out.printf(
                "%-36s %12.2f %12.2f %12.2f%n",
                "Avg latency (ms)", avgMs(latA), avgMs(latB), avgMs(latC));
        System.out.printf(
                "%-36s %12.2f %12.2f %12.2f%n",
                "P50 latency (ms)", pMs(latA, 0.5), pMs(latB, 0.5), pMs(latC, 0.5));
        System.out.printf(
                "%-36s %12.2f %12.2f %12.2f%n",
                "P90 latency (ms)", pMs(latA, 0.9), pMs(latB, 0.9), pMs(latC, 0.9));
        System.out.printf(
                "%-36s %12.2f %12.2f %12.2f%n",
                "P99 latency (ms)", pMs(latA, 0.99), pMs(latB, 0.99), pMs(latC, 0.99));

        double avgA = avgMs(latA);
        double avgB = avgMs(latB);
        double avgC = avgMs(latC);
        System.out.printf(
                "Overhead vs pure: Filter~%d%% = %+.1f%%  Filter~%d%% = %+.1f%%%n",
                selB, (avgB - avgA) / avgA * 100, selC, (avgC - avgA) / avgA * 100);
    }

    private double avgMs(long[] sorted) {
        long sum = 0;
        for (long v : sorted) {
            sum += v;
        }
        return sum / (double) sorted.length / 1_000_000.0;
    }

    private double pMs(long[] sorted, double p) {
        return sorted[(int) (sorted.length * p)] / 1_000_000.0;
    }

    // ====================== Data & Index Helpers ======================

    private void writeData(FileStoreTable table, float[][] vectors, int[] ids, int[] categories)
            throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite();
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(ids[i], categories[i], new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void buildAndCommitVectorIndex(FileStoreTable table, float[][] vectors, Range range)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vecField = table.rowType().getField(VEC_FIELD);

        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                LuminaVectorGlobalIndexerFactory.IDENTIFIER,
                                vecField,
                                options);
        for (float[] vec : vectors) {
            writer.write(vec);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        range,
                        vecField.id(),
                        LuminaVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        commitIndex(table, indexFiles);
    }

    private void buildAndCommitBTreeIndex(
            FileStoreTable table, String fieldName, int[] values, Range range) throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField field = table.rowType().getField(fieldName);

        GlobalIndexParallelWriter writer =
                (GlobalIndexParallelWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, BTreeGlobalIndexerFactory.IDENTIFIER, field, options);
        for (int i = 0; i < values.length; i++) {
            writer.write(values[i], (long) i);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        range,
                        field.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        entries);

        commitIndex(table, indexFiles);
    }

    private void commitIndex(FileStoreTable table, List<IndexFileMeta> indexFiles)
            throws Exception {
        DataIncrement inc = DataIncrement.indexIncrement(indexFiles);
        CommitMessage msg =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW, 0, null, inc, CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(msg));
        }
    }

    private static void ensureLumina() {
        if (!Lumina.isLibraryLoaded()) {
            try {
                Lumina.loadLibrary();
            } catch (LuminaException e) {
                Assumptions.assumeTrue(
                        false, "Lumina native library not available: " + e.getMessage());
            }
        }
    }
}
