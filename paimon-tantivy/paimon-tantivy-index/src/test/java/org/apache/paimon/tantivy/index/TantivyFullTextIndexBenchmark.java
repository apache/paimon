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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.tantivy.NativeLoader;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Benchmark for Tantivy full-text index comparing open-phase cost versus search-phase cost, and
 * measuring the speedup from {@link TantivySearcherPool} (searcher reuse across queries).
 *
 * <p>Run the two tests independently:
 *
 * <pre>{@code
 * # Step 1: Build index
 * mvn test -pl paimon-tantivy/paimon-tantivy-index \
 *   -Dtest=TantivyFullTextIndexBenchmark#benchmarkBuild \
 *   -DBENCHMARK_PATH=/tmp/tantivy-bench \
 *   -DBENCHMARK_NUM_DOCS=500000 \
 *   -DBENCHMARK_KEEP_INDEX=true
 *
 * # Step 2: Query (no pool vs pool)
 * mvn test -pl paimon-tantivy/paimon-tantivy-index \
 *   -Dtest=TantivyFullTextIndexBenchmark#benchmarkQuery \
 *   -DBENCHMARK_INDEX_FILE=/tmp/tantivy-bench/<uuid>/tantivy-<uuid> \
 *   -DBENCHMARK_NUM_QUERIES=500
 * }</pre>
 *
 * <h3>Parameters:</h3>
 *
 * <ul>
 *   <li>{@code BENCHMARK_PATH} — base directory for index files.
 *   <li>{@code BENCHMARK_NUM_DOCS} — number of documents to index (default 500,000).
 *   <li>{@code BENCHMARK_NUM_QUERIES} — number of measured queries per scenario (default 500).
 *   <li>{@code BENCHMARK_WARMUP_QUERIES} — JVM + index warmup queries before measurement (default
 *       max(50, numQueries/4)).
 *   <li>{@code BENCHMARK_TOP_K} — top-K results per query (default 10).
 *   <li>{@code BENCHMARK_QUERY_TERM} — search term (default "paimon").
 *   <li>{@code BENCHMARK_INDEX_FILE} — existing index file path (required for benchmarkQuery).
 *   <li>{@code BENCHMARK_KEEP_INDEX} — {@code true} to keep index after build benchmark.
 * </ul>
 */
public class TantivyFullTextIndexBenchmark {

    private static final int DEFAULT_NUM_DOCS = 500_000;
    private static final int DEFAULT_NUM_QUERIES = 500;
    private static final int DEFAULT_TOP_K = 10;
    private static final String DEFAULT_QUERY_TERM = "paimon";

    private static final String[] WORD_POOL = {
        "paimon",
        "streaming",
        "data",
        "lake",
        "flink",
        "spark",
        "table",
        "schema",
        "partition",
        "snapshot",
        "compaction",
        "bucket",
        "merge",
        "index",
        "manifest",
        "changelog",
        "append",
        "primary",
        "key",
        "lookup",
        "filter",
        "predicate",
        "format",
        "parquet",
        "orc",
        "avro",
        "writer",
        "reader",
        "catalog",
        "metastore"
    };

    private static String getEnv(String key) {
        String val = System.getProperty(key);
        if (val == null || val.isEmpty()) {
            val = System.getenv(key);
        }
        return val != null && !val.isEmpty() ? val : null;
    }

    private static int getEnvInt(String key, int defaultValue) {
        String val = getEnv(key);
        return val != null ? Integer.parseInt(val) : defaultValue;
    }

    private static void ensureNative() {
        try {
            NativeLoader.loadJni();
        } catch (Throwable t) {
            Assumptions.assumeTrue(
                    false, "Tantivy native library not available: " + t.getMessage());
        }
    }

    private static String generateDoc(Random rng, String requiredTerm, double hitRate) {
        StringBuilder sb = new StringBuilder();
        if (rng.nextDouble() < hitRate) {
            sb.append(requiredTerm).append(' ');
        }
        int wordCount = 8 + rng.nextInt(12);
        for (int i = 0; i < wordCount; i++) {
            sb.append(WORD_POOL[rng.nextInt(WORD_POOL.length)]);
            if (i < wordCount - 1) {
                sb.append(' ');
            }
        }
        return sb.toString();
    }

    /**
     * Build and write an index. Run with {@code
     * -Dtest=TantivyFullTextIndexBenchmark#benchmarkBuild}.
     */
    @Test
    public void benchmarkBuild() throws Exception {
        ensureNative();

        String benchmarkPath = getEnv("BENCHMARK_PATH");
        Assumptions.assumeTrue(benchmarkPath != null, "BENCHMARK_PATH not set.");

        int numDocs = getEnvInt("BENCHMARK_NUM_DOCS", DEFAULT_NUM_DOCS);
        boolean keepIndex = "true".equalsIgnoreCase(getEnv("BENCHMARK_KEEP_INDEX"));
        String queryTerm =
                getEnv("BENCHMARK_QUERY_TERM") != null
                        ? getEnv("BENCHMARK_QUERY_TERM")
                        : DEFAULT_QUERY_TERM;

        FileIO fileIO = new LocalFileIO();
        Path indexDir = new Path(benchmarkPath, UUID.randomUUID().toString());
        fileIO.mkdirs(indexDir);

        System.out.println("=== Tantivy Build Benchmark ===");
        System.out.printf("Path:       %s%n", indexDir);
        System.out.printf("Documents:  %,d%n", numDocs);
        System.out.printf("Query term: %s (hit rate ~10%%)%n", queryTerm);
        System.out.printf("Keep index: %s%n", keepIndex);
        System.out.println();

        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(new Path(indexDir, fileName), false);
                    }
                };

        long buildStart = System.currentTimeMillis();
        List<ResultEntry> results;
        try (TantivyFullTextGlobalIndexWriter writer =
                new TantivyFullTextGlobalIndexWriter(fileWriter)) {
            Random rng = new Random(42);
            for (int i = 0; i < numDocs; i++) {
                writer.write(BinaryString.fromString(generateDoc(rng, queryTerm, 0.1)));
            }
            results = writer.finish();
        }
        long buildEnd = System.currentTimeMillis();

        ResultEntry result = results.get(0);
        Path indexFilePath = new Path(indexDir, result.fileName());
        long fileSize = fileIO.getFileSize(indexFilePath);

        System.out.println("=== Build Results ===");
        System.out.printf("Build time:  %.2f s%n", (buildEnd - buildStart) / 1000.0);
        System.out.printf("Index file:  %s%n", indexFilePath);
        System.out.printf(
                "Index size:  %,d bytes (%.2f MB)%n", fileSize, fileSize / (1024.0 * 1024));
        System.out.printf("%nFor query benchmark:%n  -DBENCHMARK_INDEX_FILE=%s%n", indexFilePath);
        System.out.println("====================");

        if (!keepIndex) {
            fileIO.delete(indexDir, true);
        }
    }

    /**
     * Query benchmark: compares no-pool vs pool. Both scenarios share a JVM/JIT warmup phase before
     * measurement to avoid JIT compilation skewing results. Run with {@code
     * -Dtest=TantivyFullTextIndexBenchmark#benchmarkQuery}.
     */
    @Test
    public void benchmarkQuery() throws Exception {
        ensureNative();

        String existingIndexFile = getEnv("BENCHMARK_INDEX_FILE");
        Assumptions.assumeTrue(
                existingIndexFile != null,
                "BENCHMARK_INDEX_FILE not set. Run benchmarkBuild first.");

        int numQueries = getEnvInt("BENCHMARK_NUM_QUERIES", DEFAULT_NUM_QUERIES);
        int warmupQueries = getEnvInt("BENCHMARK_WARMUP_QUERIES", Math.max(50, numQueries / 4));
        int topK = getEnvInt("BENCHMARK_TOP_K", DEFAULT_TOP_K);
        String queryTerm =
                getEnv("BENCHMARK_QUERY_TERM") != null
                        ? getEnv("BENCHMARK_QUERY_TERM")
                        : DEFAULT_QUERY_TERM;

        FileIO fileIO = new LocalFileIO();
        Path indexFilePath = new Path(existingIndexFile);
        long fileSize = fileIO.getFileSize(indexFilePath);

        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(indexFilePath, fileSize, new byte[0]);
        List<GlobalIndexIOMeta> ioMetas = Collections.singletonList(ioMeta);
        GlobalIndexFileReader fileReader = meta -> fileIO.newInputStream(meta.filePath());
        FullTextSearch search = new FullTextSearch(queryTerm, topK, "text");

        System.out.println("=== Tantivy Query Benchmark ===");
        System.out.printf("Index file:  %s%n", indexFilePath);
        System.out.printf(
                "Index size:  %,d bytes (%.2f MB)%n", fileSize, fileSize / (1024.0 * 1024));
        System.out.printf(
                "Queries:     %,d measured  %,d warmup  TopK: %d  Term: \"%s\"%n",
                numQueries, warmupQueries, topK, queryTerm);
        System.out.println();

        // ── Phase 0: Shared JVM/JIT warmup (no-pool, not recorded) ───────────────
        // Drives JIT compilation of the hot path so measured runs reflect steady-state.
        System.out.printf("JVM/JIT warmup: %d queries (not measured)...%n", warmupQueries);
        TantivySearcherPool noPoolWarmup = new TantivySearcherPool(0);
        ConcurrentHashMap<String, ArchiveLayout> sharedLayoutCache = new ConcurrentHashMap<>();
        for (int i = 0; i < warmupQueries; i++) {
            try (TantivyFullTextGlobalIndexReader r =
                    new TantivyFullTextGlobalIndexReader(
                            fileReader, ioMetas, sharedLayoutCache, noPoolWarmup)) {
                r.visitFullTextSearch(search);
            }
        }
        System.out.println("Warmup done.");
        System.out.println();

        // ── Scenario 1: No pool (fresh open every query) ──────────────────────────
        System.out.printf("Running %,d queries with fresh open (no pool)...%n", numQueries);

        long[] openNs = new long[numQueries];
        long[] searchNs = new long[numQueries];
        long[] totalNs = new long[numQueries];
        // Reuse layout cache so header parsing is not part of "open" cost after warmup.
        TantivySearcherPool noPool = new TantivySearcherPool(0);

        for (int i = 0; i < numQueries; i++) {
            long t0 = System.nanoTime();
            try (TantivyFullTextGlobalIndexReader reader =
                    new TantivyFullTextGlobalIndexReader(
                            fileReader, ioMetas, sharedLayoutCache, noPool)) {
                reader.visitFullTextSearch(search);
                openNs[i] = reader.getLastOpenNanos();
                searchNs[i] = reader.getLastSearchNanos();
            }
            totalNs[i] = System.nanoTime() - t0;
        }

        // ── Scenario 2: Pool warmup ────────────────────────────────────────────────
        System.out.printf("Running %,d queries with pool (searcher reuse)...%n", numQueries);

        TantivySearcherPool pool = new TantivySearcherPool(1);

        // Populate pool with warmupQueries to reach steady state before measuring.
        for (int i = 0; i < warmupQueries; i++) {
            try (TantivyFullTextGlobalIndexReader r =
                    new TantivyFullTextGlobalIndexReader(
                            fileReader, ioMetas, sharedLayoutCache, pool)) {
                r.visitFullTextSearch(search);
            }
        }

        long[] pooledTotalNs = new long[numQueries];
        long[] pooledSearchNs = new long[numQueries];

        for (int i = 0; i < numQueries; i++) {
            long t0 = System.nanoTime();
            try (TantivyFullTextGlobalIndexReader reader =
                    new TantivyFullTextGlobalIndexReader(
                            fileReader, ioMetas, sharedLayoutCache, pool)) {
                reader.visitFullTextSearch(search);
                pooledSearchNs[i] = reader.getLastSearchNanos();
            }
            pooledTotalNs[i] = System.nanoTime() - t0;
        }

        // ── Print results ──────────────────────────────────────────────────────────
        long[] openNsSorted = sorted(openNs);
        long[] searchNsSorted = sorted(searchNs);
        long[] totalNsSorted = sorted(totalNs);
        long[] pooledTotalNsSorted = sorted(pooledTotalNs);
        long[] pooledSearchNsSorted = sorted(pooledSearchNs);

        System.out.println();
        System.out.println("=== Query Benchmark Results ===");
        System.out.printf("Index size:   %.2f MB%n", fileSize / (1024.0 * 1024));
        System.out.printf(
                "Queries:      %,d measured  %,d warmup  TopK: %d%n%n",
                numQueries, warmupQueries, topK);

        System.out.println("No-pool (fresh open + search per query):");
        printLatency("  Total  ", totalNsSorted);
        printLatency("  Open   ", openNsSorted);
        printLatency("  Search ", searchNsSorted);
        System.out.printf("  Open / Total ratio: %.1f%%%n%n", 100.0 * avg(openNs) / avg(totalNs));

        System.out.println("With pool (searcher reused from pool):");
        printLatency("  Total  ", pooledTotalNsSorted);
        printLatency("  Search ", pooledSearchNsSorted);

        System.out.println();
        double speedup = avg(totalNs) / avg(pooledTotalNs);
        double openFraction = avg(openNs) / avg(totalNs);
        System.out.printf("Speedup (avg total):  %.2fx%n", speedup);
        System.out.printf("Open-phase overhead:  %.1f%% of fresh-load total%n", openFraction * 100);
        System.out.println("===============================");
    }

    private static long[] sorted(long[] arr) {
        long[] copy = Arrays.copyOf(arr, arr.length);
        Arrays.sort(copy);
        return copy;
    }

    private static void printLatency(String label, long[] sortedNs) {
        System.out.printf(
                "%s  avg=%.2f ms  p50=%.2f ms  p90=%.2f ms  p99=%.2f ms  stddev=%.2f ms%n",
                label,
                avg(sortedNs) / 1e6,
                sortedNs[sortedNs.length / 2] / 1e6,
                sortedNs[(int) (sortedNs.length * 0.90)] / 1e6,
                sortedNs[(int) (sortedNs.length * 0.99)] / 1e6,
                stddev(sortedNs) / 1e6);
    }

    private static double avg(long[] arr) {
        long sum = 0;
        for (long v : arr) {
            sum += v;
        }
        return sum / (double) arr.length;
    }

    private static double stddev(long[] arr) {
        double mean = avg(arr);
        double sumSq = 0;
        for (long v : arr) {
            double d = v - mean;
            sumSq += d * d;
        }
        return Math.sqrt(sumSq / arr.length);
    }
}
