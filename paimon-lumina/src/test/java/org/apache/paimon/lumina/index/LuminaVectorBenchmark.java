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
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * Benchmark for Lumina vector index using {@link LuminaVectorGlobalIndexWriter} and {@link
 * LuminaVectorGlobalIndexReader}.
 *
 * <p>Build and query benchmarks are separate tests so they can be run independently.
 *
 * <p>All parameters are passed via {@code -DextraJavaTestArgs} as JVM system properties ({@code
 * -DKEY=value}). They also fall back to environment variables.
 *
 * <h3>Parameters:</h3>
 *
 * <ul>
 *   <li>{@code BENCHMARK_PATH} (required) — base path for index files (local, oss://, or pvfs://).
 *   <li>{@code BENCHMARK_NUM_VECTORS} — number of vectors (default 10,000,000).
 *   <li>{@code BENCHMARK_DIMENSION} — vector dimension (default 1024).
 *   <li>{@code BENCHMARK_ENCODING_TYPE} — encoding type: rawf32, pq, sq8 (default pq).
 *   <li>{@code BENCHMARK_DISTANCE_METRIC} — distance metric: l2, cosine, inner_product (default
 *       inner_product).
 *   <li>{@code BENCHMARK_NUM_QUERIES} — number of queries (default 1000).
 *   <li>{@code BENCHMARK_BUILD_THREADS} — DiskANN build thread count (default CPU cores).
 *   <li>{@code BENCHMARK_EF_CONSTRUCTION} — DiskANN ef_construction (default 128).
 *   <li>{@code BENCHMARK_NEIGHBOR_COUNT} — DiskANN neighbor count (default 64).
 *   <li>{@code BENCHMARK_PQ_M} — PQ sub-quantizer count (default 64).
 *   <li>{@code BENCHMARK_PQ_MAX_EPOCH} — PQ training max epoch.
 *   <li>{@code BENCHMARK_PQ_THREAD_COUNT} — PQ training thread count.
 *   <li>{@code BENCHMARK_TOP_K} — number of nearest neighbors to return (default 10). Search list
 *       size is automatically set to 1.5x TOP_K.
 *   <li>{@code BENCHMARK_INDEX_FILE} — existing index file path (required for benchmarkQuery).
 *   <li>{@code BENCHMARK_KEEP_INDEX} — {@code true} to keep index file after build benchmark.
 *   <li>{@code OSS_ENDPOINT}, {@code OSS_ACCESS_KEY_ID}, {@code OSS_ACCESS_KEY_SECRET} — for OSS.
 *   <li>{@code PVFS_URI}, {@code PVFS_ACCESS_KEY_ID}, {@code PVFS_ACCESS_KEY_SECRET} — for PVFS.
 *   <li>{@code PVFS_REGION} — (optional) PVFS region.
 * </ul>
 *
 * <h3>Example: Build (Local)</h3>
 *
 * <pre>{@code
 * mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark#benchmarkBuild \
 *   -DextraJavaTestArgs='-Xmx64g -DBENCHMARK_PATH=/tmp/lumina-benchmark -DBENCHMARK_NUM_VECTORS=100000 -DBENCHMARK_DIMENSION=128 -DBENCHMARK_ENCODING_TYPE=pq -DBENCHMARK_EF_CONSTRUCTION=32 -DBENCHMARK_NEIGHBOR_COUNT=16 -DBENCHMARK_BUILD_THREADS=16 -DBENCHMARK_KEEP_INDEX=true'
 * }</pre>
 *
 * <h3>Example: Query (Local, using index from build)</h3>
 *
 * <pre>{@code
 * mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark#benchmarkQuery \
 *   -DextraJavaTestArgs='-Xmx64g -DBENCHMARK_PATH=/tmp/lumina-benchmark -DBENCHMARK_INDEX_FILE=/tmp/lumina-benchmark/<uuid>/lumina-<uuid> -DBENCHMARK_DIMENSION=128 -DBENCHMARK_ENCODING_TYPE=pq -DBENCHMARK_NUM_QUERIES=1000'
 * }</pre>
 *
 * <h3>Example: Build (OSS)</h3>
 *
 * <pre>{@code
 * mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark#benchmarkBuild \
 *   -DextraJavaTestArgs='-Xmx64g -DBENCHMARK_PATH=oss://your-bucket/lumina-benchmark -DOSS_ENDPOINT=oss-cn-hangzhou-internal.aliyuncs.com -DOSS_ACCESS_KEY_ID=your-access-key-id -DOSS_ACCESS_KEY_SECRET=your-access-key-secret -DBENCHMARK_NUM_VECTORS=100000 -DBENCHMARK_DIMENSION=128 -DBENCHMARK_ENCODING_TYPE=pq -DBENCHMARK_EF_CONSTRUCTION=32 -DBENCHMARK_NEIGHBOR_COUNT=16 -DBENCHMARK_BUILD_THREADS=16 -DBENCHMARK_KEEP_INDEX=true'
 * }</pre>
 *
 * <h3>Example: Query (OSS)</h3>
 *
 * <pre>{@code
 * mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark#benchmarkQuery \
 *   -DextraJavaTestArgs='-Xmx64g -DBENCHMARK_PATH=oss://your-bucket/lumina-benchmark -DOSS_ENDPOINT=oss-cn-hangzhou-internal.aliyuncs.com -DOSS_ACCESS_KEY_ID=your-access-key-id -DOSS_ACCESS_KEY_SECRET=your-access-key-secret -DBENCHMARK_INDEX_FILE=oss://your-bucket/lumina-benchmark/<uuid>/lumina-<uuid> -DBENCHMARK_DIMENSION=128 -DBENCHMARK_ENCODING_TYPE=pq -DBENCHMARK_NUM_QUERIES=1000'
 * }</pre>
 *
 * <h3>Example: Build (PVFS)</h3>
 *
 * <pre>{@code
 * mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark#benchmarkBuild \
 *   -DextraJavaTestArgs='-Xmx64g -DBENCHMARK_PATH=pvfs://your-bucket/lumina-benchmark -DPVFS_URI=pvfs://your-uri -DPVFS_ACCESS_KEY_ID=your-access-key-id -DPVFS_ACCESS_KEY_SECRET=your-access-key-secret -DPVFS_REGION=cn-hangzhou -DBENCHMARK_NUM_VECTORS=100000 -DBENCHMARK_DIMENSION=128 -DBENCHMARK_ENCODING_TYPE=pq -DBENCHMARK_EF_CONSTRUCTION=32 -DBENCHMARK_NEIGHBOR_COUNT=16 -DBENCHMARK_BUILD_THREADS=16 -DBENCHMARK_KEEP_INDEX=true'
 * }</pre>
 *
 * <h3>Example: Query (PVFS)</h3>
 *
 * <pre>{@code
 * mvn test -pl paimon-lumina -Dtest=LuminaVectorBenchmark#benchmarkQuery \
 *   -DextraJavaTestArgs='-Xmx64g -DBENCHMARK_PATH=pvfs://your-bucket/lumina-benchmark -DPVFS_URI=pvfs://your-uri -DPVFS_ACCESS_KEY_ID=your-access-key-id -DPVFS_ACCESS_KEY_SECRET=your-access-key-secret -DPVFS_REGION=cn-hangzhou -DBENCHMARK_INDEX_FILE=pvfs://your-bucket/lumina-benchmark/<uuid>/lumina-<uuid> -DBENCHMARK_DIMENSION=128 -DBENCHMARK_ENCODING_TYPE=pq -DBENCHMARK_NUM_QUERIES=1000'
 * }</pre>
 */
public class LuminaVectorBenchmark {

    private static final int DEFAULT_NUM_VECTORS = 10_000_000;
    private static final int DEFAULT_DIMENSION = 1024;
    private static final int DEFAULT_TOP_K = 10;

    /** Reads a config value: system property first, then environment variable. */
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

    private static String getStorageMode(String path) {
        if (path.startsWith("oss://")) {
            return "OSS (Jindo)";
        } else if (path.startsWith("pvfs://")) {
            return "PVFS";
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
        } else if (benchmarkPath.startsWith("pvfs://")) {
            String pvfsUri = getEnv("PVFS_URI");
            String pvfsAccessKeyId = getEnv("PVFS_ACCESS_KEY_ID");
            String pvfsAccessKeySecret = getEnv("PVFS_ACCESS_KEY_SECRET");
            Assumptions.assumeTrue(
                    pvfsUri != null && pvfsAccessKeyId != null && pvfsAccessKeySecret != null,
                    "PVFS mode requires PVFS_URI, PVFS_ACCESS_KEY_ID, PVFS_ACCESS_KEY_SECRET.");

            Options fsOptions = new Options();
            fsOptions.setString(
                    "fs.pvfs.impl", "org.apache.paimon.vfs.hadoop.PaimonVirtualFileSystem");
            fsOptions.setString(
                    "fs.AbstractFileSystem.pvfs.impl", "org.apache.paimon.vfs.hadoop.Pvfs");
            fsOptions.setString("fs.pvfs.uri", pvfsUri);
            fsOptions.setString("fs.pvfs.token.provider", "dlf");
            fsOptions.setString("fs.pvfs.dlf.access-key-id", pvfsAccessKeyId);
            fsOptions.setString("fs.pvfs.dlf.access-key-secret", pvfsAccessKeySecret);
            fsOptions.setString("fs.pvfs.io-cache.enabled", "true");
            fsOptions.setString("fs.pvfs.io-cache.whitelist-path", "*");
            String pvfsRegion = getEnv("PVFS_REGION");
            if (pvfsRegion != null) {
                fsOptions.setString("fs.pvfs.dlf.region", pvfsRegion);
            }

            Configuration hadoopConf = new Configuration();
            for (Map.Entry<String, String> entry : fsOptions.toMap().entrySet()) {
                hadoopConf.set(entry.getKey(), entry.getValue());
            }
            CatalogContext context = CatalogContext.create(fsOptions, hadoopConf);
            return FileIO.get(new Path(benchmarkPath), context);
        } else {
            return new LocalFileIO();
        }
    }

    private static LuminaVectorIndexOptions createIndexOptions(
            int dimension, String distanceMetric, String encodingType) {
        int buildThreads =
                getEnvInt("BENCHMARK_BUILD_THREADS", Runtime.getRuntime().availableProcessors());
        int efConstruction = getEnvInt("BENCHMARK_EF_CONSTRUCTION", 128);
        int neighborCount = getEnvInt("BENCHMARK_NEIGHBOR_COUNT", 64);
        int pqMaxEpoch = getEnvInt("BENCHMARK_PQ_MAX_EPOCH", 0);
        int pqThreadCount = getEnvInt("BENCHMARK_PQ_THREAD_COUNT", 0);
        int pqM = getEnvInt("BENCHMARK_PQ_M", 64);

        Options luminaOpts = new Options();
        luminaOpts.setInteger(LuminaVectorIndexOptions.DIMENSION.key(), dimension);
        luminaOpts.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), distanceMetric);
        luminaOpts.setString(LuminaVectorIndexOptions.ENCODING_TYPE.key(), encodingType);
        luminaOpts.setInteger(
                LuminaVectorIndexOptions.DISKANN_BUILD_THREAD_COUNT.key(), buildThreads);
        luminaOpts.setInteger(
                LuminaVectorIndexOptions.DISKANN_BUILD_EF_CONSTRUCTION.key(), efConstruction);
        luminaOpts.setInteger(
                LuminaVectorIndexOptions.DISKANN_BUILD_NEIGHBOR_COUNT.key(), neighborCount);
        if (pqMaxEpoch > 0) {
            luminaOpts.setString("lumina.encoding.pq.max_epoch", String.valueOf(pqMaxEpoch));
        }
        if (pqThreadCount > 0) {
            luminaOpts.setString("lumina.encoding.pq.thread_count", String.valueOf(pqThreadCount));
        }
        luminaOpts.setInteger(LuminaVectorIndexOptions.ENCODING_PQ_M.key(), pqM);
        return new LuminaVectorIndexOptions(luminaOpts);
    }

    private static void printBuildConfig(
            String storageMode,
            Path indexDir,
            int numVectors,
            int dimension,
            String distanceMetric,
            String encodingType,
            boolean keepIndex) {
        int buildThreads =
                getEnvInt("BENCHMARK_BUILD_THREADS", Runtime.getRuntime().availableProcessors());
        int efConstruction = getEnvInt("BENCHMARK_EF_CONSTRUCTION", 128);
        int neighborCount = getEnvInt("BENCHMARK_NEIGHBOR_COUNT", 64);
        int pqM = getEnvInt("BENCHMARK_PQ_M", 64);
        int pqMaxEpoch = getEnvInt("BENCHMARK_PQ_MAX_EPOCH", 0);
        int pqThreadCount = getEnvInt("BENCHMARK_PQ_THREAD_COUNT", 0);

        System.out.println("=== Lumina Build Benchmark ===");
        System.out.printf("Storage:      %s%n", storageMode);
        System.out.printf("Path:         %s%n", indexDir);
        System.out.printf("Vectors:      %,d  Dimension: %d%n", numVectors, dimension);
        System.out.printf("Metric:       %s  Encoding: %s%n", distanceMetric, encodingType);
        System.out.printf(
                "Build:        threads=%d  ef_construction=%d  neighbor_count=%d%n",
                buildThreads, efConstruction, neighborCount);
        System.out.printf(
                "PQ:           m=%d  max_epoch=%s  thread_count=%s%n",
                pqM,
                pqMaxEpoch > 0 ? String.valueOf(pqMaxEpoch) : "default",
                pqThreadCount > 0 ? String.valueOf(pqThreadCount) : "default");
        System.out.printf("Keep index:   %s%n", keepIndex);
        System.out.println();
    }

    /** Benchmark: build index only. Use {@code -Dtest=LuminaVectorBenchmark#benchmarkBuild}. */
    @Test
    public void benchmarkBuild() throws Exception {
        ensureLumina();

        String benchmarkPath = getEnv("BENCHMARK_PATH");
        Assumptions.assumeTrue(
                benchmarkPath != null && !benchmarkPath.isEmpty(), "BENCHMARK_PATH not set.");

        boolean keepIndex = "true".equalsIgnoreCase(getEnv("BENCHMARK_KEEP_INDEX"));
        int numVectors = getEnvInt("BENCHMARK_NUM_VECTORS", DEFAULT_NUM_VECTORS);
        int dimension = getEnvInt("BENCHMARK_DIMENSION", DEFAULT_DIMENSION);
        String distanceMetric =
                getEnv("BENCHMARK_DISTANCE_METRIC") != null
                        ? getEnv("BENCHMARK_DISTANCE_METRIC")
                        : "inner_product";
        String encodingType =
                getEnv("BENCHMARK_ENCODING_TYPE") != null
                        ? getEnv("BENCHMARK_ENCODING_TYPE")
                        : "pq";

        boolean isRemote =
                benchmarkPath.startsWith("oss://") || benchmarkPath.startsWith("pvfs://");
        String storageMode = getStorageMode(benchmarkPath);
        FileIO fileIO = createFileIO(benchmarkPath);
        LuminaVectorIndexOptions indexOptions =
                createIndexOptions(dimension, distanceMetric, encodingType);
        DataType vectorType = new ArrayType(new FloatType());

        Path indexDir = new Path(benchmarkPath, UUID.randomUUID().toString());
        if (!isRemote) {
            fileIO.mkdirs(indexDir);
        }

        printBuildConfig(
                storageMode,
                indexDir,
                numVectors,
                dimension,
                distanceMetric,
                encodingType,
                keepIndex);

        long buildStartTime = System.currentTimeMillis();

        // When targeting OSS, build to a local temp dir first, then upload
        Path localTempDir = null;
        FileIO localFileIO = new LocalFileIO();
        final FileIO buildFileIO;
        final Path buildDir;
        if (isRemote) {
            localTempDir =
                    new Path(
                            System.getProperty("java.io.tmpdir"),
                            "lumina-bench-" + UUID.randomUUID());
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
            try (LuminaVectorGlobalIndexWriter writer =
                    new LuminaVectorGlobalIndexWriter(gFileWriter, vectorType, indexOptions)) {
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

                System.out.println("Building index (pretrain + insert + dump)...");
                long finishStart = System.currentTimeMillis();
                results = writer.finish();
                long finishEnd = System.currentTimeMillis();
                System.out.printf(
                        "Index build done in %.2f s%n", (finishEnd - finishStart) / 1000.0);
            }

            indexFileName = results.get(0).fileName();

            // Upload to OSS if needed
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
     * -Dtest=LuminaVectorBenchmark#benchmarkQuery}.
     */
    @Test
    public void benchmarkQuery() throws Exception {
        ensureLumina();

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
                        : "inner_product";
        String encodingType =
                getEnv("BENCHMARK_ENCODING_TYPE") != null
                        ? getEnv("BENCHMARK_ENCODING_TYPE")
                        : "pq";
        int numQueries = getEnvInt("BENCHMARK_NUM_QUERIES", 1000);
        int topK = getEnvInt("BENCHMARK_TOP_K", DEFAULT_TOP_K);
        String storageMode = getStorageMode(benchmarkPath);
        FileIO fileIO = createFileIO(benchmarkPath);
        LuminaVectorIndexOptions indexOptions =
                createIndexOptions(dimension, distanceMetric, encodingType);
        DataType vectorType = new ArrayType(new FloatType());

        Path existingPath = new Path(existingIndexFile);
        Path indexDir = existingPath.getParent();
        String indexFileName = existingPath.getName();

        Path indexFilePath = new Path(indexDir, indexFileName);
        long fileSize = fileIO.getFileSize(indexFilePath);

        System.out.println("=== Lumina Query Benchmark ===");
        System.out.printf("Storage:      %s%n", storageMode);
        System.out.printf("Index file:   %s%n", indexFilePath);
        System.out.printf(
                "Index size:   %,d bytes (%.2f MB)%n", fileSize, fileSize / (1024.0 * 1024));
        System.out.printf("Dimension:    %d  TopK: %d%n", dimension, topK);
        System.out.printf("Queries:      %,d%n", numQueries);
        System.out.printf("Metric:       %s  Encoding: %s%n", distanceMetric, encodingType);
        System.out.printf("Search:       list_size=%d%n", (int) (topK * 1.5));
        System.out.println();

        // Generate random query vectors
        SplittableRandom queryRandom = new SplittableRandom(99999);
        float[][] queryVectors = new float[numQueries][dimension];
        for (int i = 0; i < numQueries; i++) {
            for (int d = 0; d < dimension; d++) {
                queryVectors[i][d] = (float) queryRandom.nextDouble() * 2 - 1;
            }
        }

        LuminaIndexMeta meta = new LuminaIndexMeta(indexOptions.toLuminaOptions());
        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(indexFilePath, fileSize, meta.serialize());
        String fieldName = "vec";
        List<GlobalIndexIOMeta> ioMetas = Collections.singletonList(ioMeta);

        final FileIO benchFileIO = fileIO;
        final Path benchIndexDir = indexDir;

        System.out.printf(
                "Running %,d queries (top-%d), each with fresh index load...%n", numQueries, topK);

        long[] queryLatencies = new long[numQueries];
        long[] queryBytesRead = new long[numQueries];
        long totalQueryStart = System.currentTimeMillis();

        for (int i = 0; i < numQueries; i++) {
            VectorSearch vs = new VectorSearch(queryVectors[i], topK, fieldName);
            long queryStart = System.nanoTime();

            GlobalIndexFileReader gFileReader =
                    ioMetaArg ->
                            benchFileIO.newInputStream(
                                    new Path(benchIndexDir, ioMetaArg.filePath().getName()));
            try (LuminaVectorGlobalIndexReader reader =
                    new LuminaVectorGlobalIndexReader(
                            gFileReader, ioMetas, vectorType, indexOptions)) {
                reader.visitVectorSearch(vs);
                queryBytesRead[i] = reader.getTotalBytesRead();
            }

            long queryEnd = System.nanoTime();
            queryLatencies[i] = queryEnd - queryStart;
        }

        long totalQueryEnd = System.currentTimeMillis();
        double totalQueryTimeSec = (totalQueryEnd - totalQueryStart) / 1000.0;

        long totalBytesRead = 0;
        long minBytesRead = Long.MAX_VALUE;
        long maxBytesRead = Long.MIN_VALUE;
        for (long b : queryBytesRead) {
            totalBytesRead += b;
            minBytesRead = Math.min(minBytesRead, b);
            maxBytesRead = Math.max(maxBytesRead, b);
        }
        double avgBytesRead = totalBytesRead / (double) numQueries;

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
        System.out.println();
        System.out.println("Bytes read per query (InputStreamFileInput):");
        System.out.printf(
                "  Min:    %,d (%.2f MB)%n", minBytesRead, minBytesRead / (1024.0 * 1024));
        System.out.printf(
                "  Avg:    %,.0f (%.2f MB)%n", avgBytesRead, avgBytesRead / (1024.0 * 1024));
        System.out.printf(
                "  Max:    %,d (%.2f MB)%n", maxBytesRead, maxBytesRead / (1024.0 * 1024));
        System.out.printf(
                "  Total:  %,d (%.2f GB)%n",
                totalBytesRead, totalBytesRead / (1024.0 * 1024 * 1024));
        System.out.println("===============================");
    }
}
