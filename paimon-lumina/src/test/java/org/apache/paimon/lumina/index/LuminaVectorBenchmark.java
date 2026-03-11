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

package org.apache.paimon.lumina.index;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Benchmark for Lumina vector index. Supports both local filesystem and OSS (via Jindo).
 *
 * <p>The storage backend is determined by the {@code BENCHMARK_PATH} environment variable:
 *
 * <ul>
 *   <li>Local path (e.g. {@code /tmp/lumina-benchmark}) — uses LocalFileIO, no extra config needed.
 *   <li>OSS path (e.g. {@code oss://my-bucket/lumina-benchmark}) — uses Jindo FileIO, requires
 *       additional OSS environment variables.
 * </ul>
 *
 * <p>Environment variables:
 *
 * <ul>
 *   <li>{@code BENCHMARK_PATH} (required) — the base path for the benchmark index files. Use a
 *       local path like {@code /tmp/lumina-benchmark} for local mode, or an OSS path like {@code
 *       oss://bucket/path} for OSS mode.
 *   <li>{@code BENCHMARK_NUM_VECTORS} (optional) — number of vectors to index, defaults to
 *       10,000,000.
 *   <li>{@code BENCHMARK_DIMENSION} (optional) — vector dimension, defaults to 1024.
 *   <li>{@code BENCHMARK_INDEX_FILE} (optional) — path to an existing index file to load directly
 *       for query-only benchmarking, skipping the build phase.
 *   <li>{@code BENCHMARK_KEEP_INDEX} (optional) — set to {@code true} to keep the index file after
 *       the benchmark finishes. Defaults to {@code false} (delete after benchmark).
 *   <li>{@code OSS_ENDPOINT} — required for OSS mode, e.g. {@code
 *       oss-cn-shanghai-internal.aliyuncs.com}
 *   <li>{@code OSS_ACCESS_KEY_ID} — required for OSS mode
 *   <li>{@code OSS_ACCESS_KEY_SECRET} — required for OSS mode
 * </ul>
 *
 * <h3>Usage examples</h3>
 *
 * <p><b>Local mode (small scale test):</b>
 *
 * <pre>{@code
 * BENCHMARK_PATH=/tmp/lumina-benchmark \
 *   BENCHMARK_NUM_VECTORS=10000 \
 *   BENCHMARK_DIMENSION=128 \
 *   mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark
 * }</pre>
 *
 * <p><b>Local mode (full scale, 10M vectors, 1024 dims):</b>
 *
 * <pre>{@code
 * BENCHMARK_PATH=/tmp/lumina-benchmark \
 *   mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark \
 *   -DargLine="-Xmx8g -XX:MaxDirectMemorySize=8g"
 * }</pre>
 *
 * <p><b>OSS mode (via Jindo):</b>
 *
 * <pre>{@code
 * BENCHMARK_PATH=oss://my-bucket/lumina-benchmark \
 *   BENCHMARK_NUM_VECTORS=10000000 \
 *   BENCHMARK_DIMENSION=1024 \
 *   OSS_ENDPOINT=oss-cn-shanghai-internal.aliyuncs.com \
 *   OSS_ACCESS_KEY_ID=<your-access-key-id> \
 *   OSS_ACCESS_KEY_SECRET=<your-access-key-secret> \
 *   mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark \
 *   -DargLine="-Xmx8g -XX:MaxDirectMemorySize=8g"
 * }</pre>
 *
 * <p><b>Build index and keep the file for later use:</b>
 *
 * <pre>{@code
 * BENCHMARK_PATH=/tmp/lumina-benchmark \
 *   BENCHMARK_NUM_VECTORS=10000 \
 *   BENCHMARK_DIMENSION=128 \
 *   BENCHMARK_KEEP_INDEX=true \
 *   mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark
 * }</pre>
 *
 * <p><b>Query-only mode (load an existing index file, skip build):</b>
 *
 * <pre>{@code
 * BENCHMARK_PATH=/tmp/lumina-benchmark \
 *   BENCHMARK_DIMENSION=128 \
 *   BENCHMARK_INDEX_FILE=/tmp/lumina-benchmark/<uuid>/lumina-benchmark-<uuid> \
 *   mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark
 * }</pre>
 */
public class LuminaVectorBenchmark {

    private static final int DEFAULT_NUM_VECTORS = 10_000_000;
    private static final int DEFAULT_DIMENSION = 1024;
    private static final int NUM_QUERIES = 1000;
    private static final int TOP_K = 10;

    private static int getEnvInt(String key, int defaultValue) {
        String val = System.getenv(key);
        return val != null && !val.isEmpty() ? Integer.parseInt(val) : defaultValue;
    }

    private static String getEnv(String key) {
        String val = System.getenv(key);
        return val != null && !val.isEmpty() ? val : null;
    }

    /**
     * Computes max vectors per batch, limited by ByteBuffer (Integer.MAX_VALUE bytes). Each vector
     * occupies {@code dimension * 4} bytes.
     */
    private static int computeBatchSize(int dimension) {
        long bytesPerVector = (long) dimension * Float.BYTES;
        return (int) Math.min(500_000, Integer.MAX_VALUE / bytesPerVector);
    }

    @Test
    public void benchmark() throws Exception {
        // Check Lumina native library
        if (!Lumina.isLibraryLoaded()) {
            try {
                Lumina.loadLibrary();
            } catch (LuminaException e) {
                Assumptions.assumeTrue(
                        false, "Lumina native library not available: " + e.getMessage());
            }
        }

        // Resolve configurable parameters
        int numVectors = getEnvInt("BENCHMARK_NUM_VECTORS", DEFAULT_NUM_VECTORS);
        int dimension = getEnvInt("BENCHMARK_DIMENSION", DEFAULT_DIMENSION);
        int batchSize = computeBatchSize(dimension);
        int pretrainSampleSize = Math.min(100_000, numVectors);
        String existingIndexFile = getEnv("BENCHMARK_INDEX_FILE");
        boolean keepIndex = "true".equalsIgnoreCase(getEnv("BENCHMARK_KEEP_INDEX"));

        // Resolve storage path
        String benchmarkPath = System.getenv("BENCHMARK_PATH");
        Assumptions.assumeTrue(
                benchmarkPath != null && !benchmarkPath.isEmpty(),
                "BENCHMARK_PATH not set. "
                        + "Set BENCHMARK_PATH to a local path (e.g. /tmp/lumina-benchmark) "
                        + "or an OSS path (e.g. oss://bucket/path) to run this benchmark.");

        FileIO fileIO;
        boolean isOss = benchmarkPath.startsWith("oss://");

        if (isOss) {
            String endpoint = System.getenv("OSS_ENDPOINT");
            String accessKeyId = System.getenv("OSS_ACCESS_KEY_ID");
            String accessKeySecret = System.getenv("OSS_ACCESS_KEY_SECRET");
            Assumptions.assumeTrue(
                    endpoint != null && accessKeyId != null && accessKeySecret != null,
                    "OSS mode requires OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET.");

            Options options = new Options();
            options.setString("fs.oss.endpoint", endpoint);
            options.setString("fs.oss.accessKeyId", accessKeyId);
            options.setString("fs.oss.accessKeySecret", accessKeySecret);
            CatalogContext context = CatalogContext.create(options);

            fileIO = FileIO.get(new Path(benchmarkPath), context);
        } else {
            fileIO = new LocalFileIO();
        }

        // Determine index file path
        Path indexDir;
        Path indexFilePath;
        boolean skipBuild;

        if (existingIndexFile != null) {
            indexFilePath = new Path(existingIndexFile);
            indexDir = indexFilePath.getParent();
            skipBuild = true;
        } else {
            indexDir = new Path(benchmarkPath, UUID.randomUUID().toString());
            if (!isOss) {
                fileIO.mkdirs(indexDir);
            }
            indexFilePath = new Path(indexDir, "lumina-benchmark-" + UUID.randomUUID());
            skipBuild = false;
        }

        String storageMode = isOss ? "OSS (Jindo)" : "Local";
        System.out.println("=== Lumina Vector Benchmark ===");
        System.out.printf("Storage:      %s%n", storageMode);
        System.out.printf("Path:         %s%n", indexDir);
        System.out.printf(
                "Vectors:      %,d  Dimension: %d  TopK: %d%n", numVectors, dimension, TOP_K);
        System.out.printf(
                "Batch size:   %,d  Pretrain sample: %,d%n", batchSize, pretrainSampleSize);
        System.out.printf("Keep index:   %s%n", keepIndex);
        if (skipBuild) {
            System.out.printf("Index file:   %s (skip build)%n", indexFilePath);
        }
        System.out.println();

        // Build lumina options
        Options luminaOpts = new Options();
        luminaOpts.setInteger(LuminaVectorIndexOptions.DIMENSION.key(), dimension);
        luminaOpts.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
        luminaOpts.setString(LuminaVectorIndexOptions.ENCODING_TYPE.key(), "pq");
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(luminaOpts);
        Map<String, String> nativeOptions = indexOptions.toLuminaOptions();

        try {
            // ============================================================
            // Phase 1: Build index (skipped if loading existing file)
            // ============================================================
            if (!skipBuild) {
                System.out.println("--- Phase 1: Build Index ---");
                long buildStartTime = System.currentTimeMillis();
                Random random = new Random(42);

                try (LuminaIndex index =
                        LuminaIndex.createForBuild(
                                dimension, LuminaVectorMetric.L2, nativeOptions)) {

                    // Pretrain with a sample
                    System.out.printf(
                            "Pretraining with %,d sample vectors...%n", pretrainSampleSize);
                    long pretrainStart = System.currentTimeMillis();
                    ByteBuffer pretrainBuffer =
                            LuminaIndex.allocateVectorBuffer(pretrainSampleSize, dimension);
                    FloatBuffer pretrainFloat = pretrainBuffer.asFloatBuffer();
                    for (int i = 0; i < pretrainSampleSize; i++) {
                        for (int d = 0; d < dimension; d++) {
                            pretrainFloat.put(random.nextFloat() * 2 - 1);
                        }
                    }
                    index.pretrain(pretrainBuffer, pretrainSampleSize);
                    long pretrainEnd = System.currentTimeMillis();
                    System.out.printf(
                            "Pretrain done in %.2f s%n", (pretrainEnd - pretrainStart) / 1000.0);

                    // Insert in batches
                    int totalInserted = 0;
                    int batchNum = 0;
                    Random insertRandom = new Random(12345);

                    while (totalInserted < numVectors) {
                        int batchCount = Math.min(batchSize, numVectors - totalInserted);
                        batchNum++;
                        System.out.printf(
                                "Inserting batch %d: %,d vectors (total: %,d / %,d)...%n",
                                batchNum, batchCount, totalInserted + batchCount, numVectors);

                        long batchStart = System.currentTimeMillis();
                        ByteBuffer vectorBuffer =
                                LuminaIndex.allocateVectorBuffer(batchCount, dimension);
                        FloatBuffer floatView = vectorBuffer.asFloatBuffer();
                        for (int i = 0; i < batchCount; i++) {
                            for (int d = 0; d < dimension; d++) {
                                floatView.put(insertRandom.nextFloat() * 2 - 1);
                            }
                        }

                        ByteBuffer idBuffer = LuminaIndex.allocateIdBuffer(batchCount);
                        LongBuffer longView = idBuffer.asLongBuffer();
                        for (int i = 0; i < batchCount; i++) {
                            longView.put(i, (long) totalInserted + i);
                        }

                        index.insertBatch(vectorBuffer, idBuffer, batchCount);
                        totalInserted += batchCount;

                        long batchEnd = System.currentTimeMillis();
                        System.out.printf(
                                "  Batch %d done in %.2f s (%.0f vectors/s)%n",
                                batchNum,
                                (batchEnd - batchStart) / 1000.0,
                                batchCount / ((batchEnd - batchStart) / 1000.0));
                    }

                    // Dump index to storage
                    System.out.println("Dumping index to storage...");
                    long dumpStart = System.currentTimeMillis();
                    try (PositionOutputStream out = fileIO.newOutputStream(indexFilePath, false)) {
                        index.dump(new LuminaVectorGlobalIndexWriter.OutputStreamFileOutput(out));
                        out.flush();
                    }
                    long dumpEnd = System.currentTimeMillis();
                    long buildFileSize = fileIO.getFileSize(indexFilePath);
                    System.out.printf(
                            "Dump done in %.2f s, file size: %,d bytes (%.2f GB)%n",
                            (dumpEnd - dumpStart) / 1000.0,
                            buildFileSize,
                            buildFileSize / (1024.0 * 1024 * 1024));
                }

                long buildEndTime = System.currentTimeMillis();
                System.out.printf(
                        "Total build time: %.2f s%n", (buildEndTime - buildStartTime) / 1000.0);
                if (keepIndex) {
                    System.out.printf("Index file kept at: %s%n", indexFilePath);
                }
                System.out.println();
            } else {
                System.out.println("--- Phase 1: Build Index (SKIPPED, loading existing) ---");
                System.out.println();
            }

            // ============================================================
            // Phase 2: Query benchmark
            // ============================================================
            System.out.println("--- Phase 2: Query Benchmark ---");

            long fileSize = fileIO.getFileSize(indexFilePath);
            SeekableInputStream in = fileIO.newInputStream(indexFilePath);

            try (LuminaIndex searchIndex =
                    LuminaIndex.fromStream(
                            new LuminaVectorGlobalIndexReader.InputStreamFileInput(in),
                            fileSize,
                            dimension,
                            LuminaVectorMetric.L2,
                            nativeOptions)) {

                System.out.printf("Index loaded, size: %,d vectors%n", searchIndex.size());

                // Generate query vectors
                Random queryRandom = new Random(99999);
                float[][] queryVectors = new float[NUM_QUERIES][dimension];
                for (int i = 0; i < NUM_QUERIES; i++) {
                    for (int d = 0; d < dimension; d++) {
                        queryVectors[i][d] = queryRandom.nextFloat() * 2 - 1;
                    }
                }

                // Warmup
                System.out.println("Warming up...");
                for (int i = 0; i < Math.min(10, NUM_QUERIES); i++) {
                    float[] distances = new float[TOP_K];
                    long[] labels = new long[TOP_K];
                    searchIndex.search(queryVectors[i], 1, TOP_K, distances, labels, nativeOptions);
                }

                // Benchmark queries with CPU and memory tracking
                System.out.printf("Running %,d queries (top-%d)...%n", NUM_QUERIES, TOP_K);
                ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
                Runtime runtime = Runtime.getRuntime();

                // Snapshot memory before queries
                runtime.gc();
                long memBeforeUsed = runtime.totalMemory() - runtime.freeMemory();
                MemoryUsage heapBefore = memoryMXBean.getHeapMemoryUsage();
                MemoryUsage nonHeapBefore = memoryMXBean.getNonHeapMemoryUsage();

                // Snapshot CPU time before queries
                long cpuTimeBefore = threadMXBean.getCurrentThreadCpuTime();
                long userTimeBefore = threadMXBean.getCurrentThreadUserTime();

                long[] queryLatencies = new long[NUM_QUERIES];
                long totalQueryStart = System.currentTimeMillis();

                for (int i = 0; i < NUM_QUERIES; i++) {
                    float[] distances = new float[TOP_K];
                    long[] labels = new long[TOP_K];

                    long queryStart = System.nanoTime();
                    searchIndex.search(queryVectors[i], 1, TOP_K, distances, labels, nativeOptions);
                    long queryEnd = System.nanoTime();

                    queryLatencies[i] = queryEnd - queryStart;
                }

                long totalQueryEnd = System.currentTimeMillis();
                double totalQueryTimeSec = (totalQueryEnd - totalQueryStart) / 1000.0;

                // Snapshot CPU time after queries
                long cpuTimeAfter = threadMXBean.getCurrentThreadCpuTime();
                long userTimeAfter = threadMXBean.getCurrentThreadUserTime();
                double cpuTimeMs = (cpuTimeAfter - cpuTimeBefore) / 1_000_000.0;
                double userTimeMs = (userTimeAfter - userTimeBefore) / 1_000_000.0;
                double sysTimeMs = cpuTimeMs - userTimeMs;
                double cpuUtilization = cpuTimeMs / (totalQueryTimeSec * 1000.0) * 100.0;

                // Snapshot memory after queries
                long memAfterUsed = runtime.totalMemory() - runtime.freeMemory();
                MemoryUsage heapAfter = memoryMXBean.getHeapMemoryUsage();
                MemoryUsage nonHeapAfter = memoryMXBean.getNonHeapMemoryUsage();

                // Compute latency statistics
                Arrays.sort(queryLatencies);
                double avgLatencyMs = 0;
                for (long lat : queryLatencies) {
                    avgLatencyMs += lat / 1_000_000.0;
                }
                avgLatencyMs /= NUM_QUERIES;

                double p50Ms = queryLatencies[NUM_QUERIES / 2] / 1_000_000.0;
                double p90Ms = queryLatencies[(int) (NUM_QUERIES * 0.9)] / 1_000_000.0;
                double p95Ms = queryLatencies[(int) (NUM_QUERIES * 0.95)] / 1_000_000.0;
                double p99Ms = queryLatencies[(int) (NUM_QUERIES * 0.99)] / 1_000_000.0;
                double minMs = queryLatencies[0] / 1_000_000.0;
                double maxMs = queryLatencies[NUM_QUERIES - 1] / 1_000_000.0;
                double rps = NUM_QUERIES / totalQueryTimeSec;

                System.out.println();
                System.out.println("=== Query Benchmark Results ===");
                System.out.printf("Storage:          %s%n", storageMode);
                System.out.printf(
                        "Index file size:  %,d bytes (%.2f MB)%n",
                        fileSize, fileSize / (1024.0 * 1024));
                System.out.printf("Total queries:    %,d%n", NUM_QUERIES);
                System.out.printf("Total time:       %.2f s%n", totalQueryTimeSec);
                System.out.printf("RPS:              %.2f queries/s%n", rps);
                System.out.println();
                System.out.println("Latency (ms):");
                System.out.printf("  Min:    %.3f%n", minMs);
                System.out.printf("  Avg:    %.3f%n", avgLatencyMs);
                System.out.printf("  P50:    %.3f%n", p50Ms);
                System.out.printf("  P90:    %.3f%n", p90Ms);
                System.out.printf("  P95:    %.3f%n", p95Ms);
                System.out.printf("  P99:    %.3f%n", p99Ms);
                System.out.printf("  Max:    %.3f%n", maxMs);
                System.out.println();
                System.out.println("CPU:");
                System.out.printf("  Total CPU time:   %.2f ms%n", cpuTimeMs);
                System.out.printf("  User time:        %.2f ms%n", userTimeMs);
                System.out.printf("  System time:      %.2f ms%n", sysTimeMs);
                System.out.printf("  CPU per query:    %.3f ms%n", cpuTimeMs / NUM_QUERIES);
                System.out.printf("  CPU utilization:  %.1f%%%n", cpuUtilization);
                System.out.println();
                System.out.println("Memory:");
                System.out.printf(
                        "  Heap used:        %,d MB (before) -> %,d MB (after)%n",
                        heapBefore.getUsed() / (1024 * 1024), heapAfter.getUsed() / (1024 * 1024));
                System.out.printf(
                        "  Non-heap used:    %,d MB (before) -> %,d MB (after)%n",
                        nonHeapBefore.getUsed() / (1024 * 1024),
                        nonHeapAfter.getUsed() / (1024 * 1024));
                System.out.printf(
                        "  JVM total:        %,d MB  Max: %,d MB%n",
                        runtime.totalMemory() / (1024 * 1024), runtime.maxMemory() / (1024 * 1024));
                System.out.printf(
                        "  Memory delta:     %+,d KB%n", (memAfterUsed - memBeforeUsed) / 1024);
                System.out.println("===============================");
            } finally {
                in.close();
            }
        } finally {
            if (!keepIndex && !skipBuild) {
                System.out.println("\nCleaning up...");
                fileIO.delete(indexFilePath, false);
                fileIO.delete(indexDir, true);
                System.out.println("Done.");
            } else {
                System.out.printf("%nIndex file retained at: %s%n", indexFilePath);
            }
        }
    }
}
