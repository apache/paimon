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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.elasticsearch.index.model.ESVectorIndexOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import org.elasticsearch.vectorindex.model.VectorIndexMeta;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * Benchmark for ES (DiskBBQ) vector index.
 *
 * <p>Build and query benchmarks are separate tests so they can be run independently. Parameters are
 * passed via JVM system properties or environment variables, matching the format of {@code
 * LuminaVectorBenchmark} for direct comparison.
 *
 * <h3>Parameters:</h3>
 *
 * <ul>
 *   <li>{@code BENCHMARK_PATH} (required) — base path for index files (local or oss://).
 *   <li>{@code BENCHMARK_NUM_VECTORS} — number of vectors (default 10,000,000).
 *   <li>{@code BENCHMARK_DIMENSION} — vector dimension (default 1024).
 *   <li>{@code BENCHMARK_DISTANCE_METRIC} — l2, cosine, inner_product (default l2).
 *   <li>{@code BENCHMARK_VECTORS_PER_CLUSTER} — IVF vectors per cluster (default 1000).
 *   <li>{@code BENCHMARK_CENTROIDS_PER_PARENT} — hierarchical KMeans centroids (default 8).
 *   <li>{@code BENCHMARK_NUM_QUERIES} — number of queries (default 1000).
 *   <li>{@code BENCHMARK_TOP_K} — number of nearest neighbors (default 10).
 *   <li>{@code BENCHMARK_INDEX_FILE} — existing index file path (required for benchmarkQuery).
 *   <li>{@code BENCHMARK_KEEP_INDEX} — {@code true} to keep index file after build.
 *   <li>{@code OSS_ENDPOINT}, {@code OSS_ACCESS_KEY_ID}, {@code OSS_ACCESS_KEY_SECRET} — for OSS.
 * </ul>
 */
public class ESVectorBenchmark {

    private static final int DEFAULT_NUM_VECTORS = 10_000_000;
    private static final int DEFAULT_DIMENSION = 1024;
    private static final int DEFAULT_TOP_K = 10;

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

    private static final int UPLOAD_BUFFER_SIZE = 8 * 1024 * 1024;

    private static void uploadFile(FileIO srcIO, Path src, FileIO dstIO, Path dst)
            throws IOException {
        try (SeekableInputStream in = srcIO.newInputStream(src);
                PositionOutputStream out = dstIO.newOutputStream(dst, false)) {
            byte[] buf = new byte[UPLOAD_BUFFER_SIZE];
            int read;
            while ((read = in.read(buf)) != -1) {
                out.write(buf, 0, read);
            }
            out.flush();
        }
    }

    private static String getStorageMode(String path) {
        if (path.startsWith("oss://")) {
            return "OSS";
        } else {
            return "Local";
        }
    }

    private static FileIO createFileIO(String benchmarkPath) throws IOException {
        if (benchmarkPath.startsWith("oss://")) {
            String endpoint = getEnv("OSS_ENDPOINT");
            String accessKeyId = getEnv("OSS_ACCESS_KEY_ID");
            String accessKeySecret = getEnv("OSS_ACCESS_KEY_SECRET");
            Assumptions.assumeTrue(
                    endpoint != null && accessKeyId != null && accessKeySecret != null,
                    "OSS mode requires OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET.");

            Options fsOptions = new Options();
            fsOptions.setString("fs.oss.endpoint", endpoint);
            fsOptions.setString("fs.oss.accessKeyId", accessKeyId);
            fsOptions.setString("fs.oss.accessKeySecret", accessKeySecret);
            CatalogContext context = CatalogContext.create(fsOptions);
            return FileIO.get(new Path(benchmarkPath), context);
        } else {
            return new LocalFileIO();
        }
    }

    private static ESVectorIndexOptions createIndexOptions(int dimension, String distanceMetric) {
        int vectorsPerCluster = getEnvInt("BENCHMARK_VECTORS_PER_CLUSTER", 1000);
        int centroidsPerParent = getEnvInt("BENCHMARK_CENTROIDS_PER_PARENT", 8);

        Options opts = new Options();
        opts.setInteger(ESVectorIndexOptions.DIMENSION.key(), dimension);
        opts.setString(ESVectorIndexOptions.DISTANCE_METRIC.key(), distanceMetric);
        opts.setInteger(ESVectorIndexOptions.VECTORS_PER_CLUSTER.key(), vectorsPerCluster);
        opts.setInteger(
                ESVectorIndexOptions.CENTROIDS_PER_PARENT_CLUSTER.key(), centroidsPerParent);
        return new ESVectorIndexOptions(opts);
    }

    /** Benchmark: build index only. Use {@code -Dtest=ESVectorBenchmark#benchmarkBuild}. */
    @Test
    public void benchmarkBuild() throws Exception {
        String benchmarkPath = getEnv("BENCHMARK_PATH");
        Assumptions.assumeTrue(
                benchmarkPath != null && !benchmarkPath.isEmpty(), "BENCHMARK_PATH not set.");

        boolean keepIndex = "true".equalsIgnoreCase(getEnv("BENCHMARK_KEEP_INDEX"));
        int numVectors = getEnvInt("BENCHMARK_NUM_VECTORS", DEFAULT_NUM_VECTORS);
        int dimension = getEnvInt("BENCHMARK_DIMENSION", DEFAULT_DIMENSION);
        String distanceMetric =
                getEnv("BENCHMARK_DISTANCE_METRIC") != null
                        ? getEnv("BENCHMARK_DISTANCE_METRIC")
                        : "l2";

        boolean isRemote = benchmarkPath.startsWith("oss://");
        String storageMode = getStorageMode(benchmarkPath);
        FileIO fileIO = createFileIO(benchmarkPath);
        ESVectorIndexOptions indexOptions = createIndexOptions(dimension, distanceMetric);
        DataType vectorType = new ArrayType(new FloatType());

        Path indexDir = new Path(benchmarkPath, UUID.randomUUID().toString());
        if (!isRemote) {
            fileIO.mkdirs(indexDir);
        }

        int vectorsPerCluster = getEnvInt("BENCHMARK_VECTORS_PER_CLUSTER", 1000);
        int centroidsPerParent = getEnvInt("BENCHMARK_CENTROIDS_PER_PARENT", 8);

        System.out.println("=== ES (DiskBBQ) Build Benchmark ===");
        System.out.printf("Storage:      %s%n", storageMode);
        System.out.printf("Path:         %s%n", indexDir);
        System.out.printf("Vectors:      %,d  Dimension: %d%n", numVectors, dimension);
        System.out.printf("Metric:       %s%n", distanceMetric);
        System.out.printf(
                "DiskBBQ:      vectors_per_cluster=%d  centroids_per_parent=%d%n",
                vectorsPerCluster, centroidsPerParent);
        System.out.printf("Keep index:   %s%n", keepIndex);
        System.out.println();

        long buildStartTime = System.currentTimeMillis();

        Path localTempDir = null;
        FileIO localFileIO = new LocalFileIO();
        final FileIO buildFileIO;
        final Path buildDir;
        if (isRemote) {
            localTempDir =
                    new Path(System.getProperty("java.io.tmpdir"), "es-bench-" + UUID.randomUUID());
            localFileIO.mkdirs(localTempDir);
            buildFileIO = localFileIO;
            buildDir = localTempDir;
            System.out.printf("Building to local temp: %s%n", localTempDir);
        } else {
            buildFileIO = fileIO;
            buildDir = indexDir;
        }

        String indexFileName;
        try {
            final Path finalBuildDir = buildDir;
            GlobalIndexFileWriter gFileWriter =
                    new GlobalIndexFileWriter() {
                        @Override
                        public String newFileName(String prefix) {
                            return prefix + "-" + UUID.randomUUID();
                        }

                        @Override
                        public PositionOutputStream newOutputStream(String fileName)
                                throws IOException {
                            return buildFileIO.newOutputStream(
                                    new Path(finalBuildDir, fileName), false);
                        }
                    };

            List<ResultEntry> results;
            try (ESVectorGlobalIndexWriter writer =
                    new ESVectorGlobalIndexWriter(gFileWriter, "vec", vectorType, indexOptions)) {
                System.out.printf("Writing %,d vectors...%n", numVectors);
                long writeStart = System.currentTimeMillis();
                SplittableRandom insertRandom = new SplittableRandom(12345);
                float[] vec = new float[dimension];
                for (int i = 0; i < numVectors; i++) {
                    for (int d = 0; d < dimension; d++) {
                        vec[d] = (float) insertRandom.nextDouble() * 2 - 1;
                    }
                    writer.write(vec);
                }
                long writeEnd = System.currentTimeMillis();
                System.out.printf(
                        "Write done in %.2f s (%.0f vectors/s)%n",
                        (writeEnd - writeStart) / 1000.0,
                        numVectors / ((writeEnd - writeStart) / 1000.0));

                System.out.println("Building index (DiskBBQ IVF + quantization)...");
                long finishStart = System.currentTimeMillis();
                results = writer.finish();
                long finishEnd = System.currentTimeMillis();
                System.out.printf(
                        "Index build done in %.2f s%n", (finishEnd - finishStart) / 1000.0);
            }

            indexFileName = results.get(0).fileName();

            if (isRemote) {
                Path localFile = new Path(buildDir, indexFileName);
                Path remoteFile = new Path(indexDir, indexFileName);
                long localSize = localFileIO.getFileSize(localFile);
                System.out.printf(
                        "Uploading to OSS: %s (%,d bytes, %.2f GB)...%n",
                        remoteFile, localSize, localSize / (1024.0 * 1024 * 1024));
                long uploadStart = System.currentTimeMillis();
                uploadFile(localFileIO, localFile, fileIO, remoteFile);
                long uploadEnd = System.currentTimeMillis();
                double uploadSec = (uploadEnd - uploadStart) / 1000.0;
                System.out.printf(
                        "Upload done in %.2f s (%.2f MB/s)%n",
                        uploadSec, localSize / (1024.0 * 1024) / uploadSec);
            }

            Path indexFilePath = new Path(indexDir, indexFileName);
            long buildFileSize = fileIO.getFileSize(indexFilePath);
            long buildEndTime = System.currentTimeMillis();

            System.out.println();
            System.out.println("=== Build Benchmark Results ===");
            System.out.printf(
                    "Total build time: %.2f s%n", (buildEndTime - buildStartTime) / 1000.0);
            System.out.printf(
                    "Index file: %s (%,d bytes, %.2f GB)%n",
                    indexFilePath, buildFileSize, buildFileSize / (1024.0 * 1024 * 1024));
            System.out.printf(
                    "%nUse this for query benchmark:%n  -DBENCHMARK_INDEX_FILE=%s%n",
                    indexFilePath);
            System.out.println("===============================");
        } finally {
            if (localTempDir != null) {
                localFileIO.delete(localTempDir, true);
            }
            if (!keepIndex) {
                System.out.println("\nCleaning up...");
                fileIO.delete(indexDir, true);
                System.out.println("Done.");
            }
        }
    }

    /**
     * Benchmark: query only. Requires {@code BENCHMARK_INDEX_FILE}. Use {@code
     * -Dtest=ESVectorBenchmark#benchmarkQuery}.
     */
    @Test
    public void benchmarkQuery() throws Exception {
        String benchmarkPath = getEnv("BENCHMARK_PATH");
        Assumptions.assumeTrue(
                benchmarkPath != null && !benchmarkPath.isEmpty(), "BENCHMARK_PATH not set.");

        String existingIndexFile = getEnv("BENCHMARK_INDEX_FILE");
        Assumptions.assumeTrue(
                existingIndexFile != null && !existingIndexFile.isEmpty(),
                "BENCHMARK_INDEX_FILE not set. Run benchmarkBuild first.");

        int dimension = getEnvInt("BENCHMARK_DIMENSION", DEFAULT_DIMENSION);
        String distanceMetric =
                getEnv("BENCHMARK_DISTANCE_METRIC") != null
                        ? getEnv("BENCHMARK_DISTANCE_METRIC")
                        : "l2";
        int numQueries = getEnvInt("BENCHMARK_NUM_QUERIES", 1000);
        int topK = getEnvInt("BENCHMARK_TOP_K", DEFAULT_TOP_K);
        String storageMode = getStorageMode(benchmarkPath);
        FileIO fileIO = createFileIO(benchmarkPath);
        ESVectorIndexOptions indexOptions = createIndexOptions(dimension, distanceMetric);
        DataType vectorType = new ArrayType(new FloatType());

        Path existingPath = new Path(existingIndexFile);
        Path indexDir = existingPath.getParent();
        String indexFileName = existingPath.getName();

        Path indexFilePath = new Path(indexDir, indexFileName);
        long fileSize = fileIO.getFileSize(indexFilePath);

        System.out.println("=== ES (DiskBBQ) Query Benchmark ===");
        System.out.printf("Storage:      %s%n", storageMode);
        System.out.printf("Index file:   %s%n", indexFilePath);
        System.out.printf(
                "Index size:   %,d bytes (%.2f MB)%n", fileSize, fileSize / (1024.0 * 1024));
        System.out.printf("Dimension:    %d  TopK: %d%n", dimension, topK);
        System.out.printf("Queries:      %,d%n", numQueries);
        System.out.printf("Metric:       %s%n", distanceMetric);
        System.out.println();

        SplittableRandom queryRandom = new SplittableRandom(99999);
        float[][] queryVectors = new float[numQueries][dimension];
        for (int i = 0; i < numQueries; i++) {
            for (int d = 0; d < dimension; d++) {
                queryVectors[i][d] = (float) queryRandom.nextDouble() * 2 - 1;
            }
        }

        VectorIndexMeta meta = new VectorIndexMeta(indexOptions.toVectorIndexConfig(), 0);
        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(indexFilePath, fileSize, meta.serialize());
        String fieldName = "vec";
        List<GlobalIndexIOMeta> ioMetas = Collections.singletonList(ioMeta);

        final FileIO benchFileIO = fileIO;
        final Path benchIndexDir = indexDir;

        System.out.printf(
                "Running %,d queries (top-%d), each with fresh index load...%n", numQueries, topK);

        long[] queryLatencies = new long[numQueries];
        long totalQueryStart = System.currentTimeMillis();

        for (int i = 0; i < numQueries; i++) {
            VectorSearch vs = new VectorSearch(queryVectors[i], topK, fieldName);
            long queryStart = System.nanoTime();

            GlobalIndexFileReader gFileReader =
                    ioMetaArg ->
                            benchFileIO.newInputStream(
                                    new Path(benchIndexDir, ioMetaArg.filePath().getName()));
            try (ESVectorGlobalIndexReader reader =
                    new ESVectorGlobalIndexReader(gFileReader, ioMetas, vectorType, indexOptions)) {
                reader.visitVectorSearch(vs);
            }

            long queryEnd = System.nanoTime();
            queryLatencies[i] = queryEnd - queryStart;
        }

        long totalQueryEnd = System.currentTimeMillis();
        double totalQueryTimeSec = (totalQueryEnd - totalQueryStart) / 1000.0;

        Arrays.sort(queryLatencies);
        double avgLatencyMs = 0;
        for (long lat : queryLatencies) {
            avgLatencyMs += lat / 1_000_000.0;
        }
        avgLatencyMs /= numQueries;

        double p50Ms = queryLatencies[numQueries / 2] / 1_000_000.0;
        double p90Ms = queryLatencies[(int) (numQueries * 0.9)] / 1_000_000.0;
        double p95Ms = queryLatencies[(int) (numQueries * 0.95)] / 1_000_000.0;
        double p99Ms = queryLatencies[(int) (numQueries * 0.99)] / 1_000_000.0;
        double minMs = queryLatencies[0] / 1_000_000.0;
        double maxMs = queryLatencies[numQueries - 1] / 1_000_000.0;
        double rps = numQueries / totalQueryTimeSec;

        System.out.println();
        System.out.println("=== Query Benchmark Results ===");
        System.out.printf("Storage:          %s%n", storageMode);
        System.out.printf(
                "Index file size:  %,d bytes (%.2f MB)%n", fileSize, fileSize / (1024.0 * 1024));
        System.out.printf("Total queries:    %,d%n", numQueries);
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
        System.out.println("===============================");
    }
}
