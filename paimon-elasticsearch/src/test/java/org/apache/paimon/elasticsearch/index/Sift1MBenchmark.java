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

import org.apache.paimon.elasticsearch.index.model.ESScoredGlobalIndexResult;
import org.apache.paimon.elasticsearch.index.model.ESVectorIndexOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** SIFT1M benchmark: DiskBBQ-serial vs DiskBBQ-parallel vs Lumina recall and latency. */
public class Sift1MBenchmark {

    private static final String SIFT_DIR =
            "/root/changfeng/idea_ssh/es-openstore-study/aliyun-elasticsearch"
                    + "/a-pack/plugin/shadow-replicas/src/test/plugin-metadata/sift128";
    private static final int DIM = 128;
    private static final int NUM_QUERIES = 100;
    private static final int TOP_K = 10;
    private static final int WARMUP = 5;
    private static final int BENCH_ROUNDS = 3;

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void runBenchmark() throws Exception {
        System.out.println("============================================================");
        System.out.println("  SIFT1M Benchmark: DiskBBQ-Serial vs DiskBBQ-Parallel");
        System.out.println("  Dataset: SIFT1M (1,000,000 vectors, 128-dim, L2)");
        System.out.println("============================================================\n");

        // Load SIFT data
        int numBase = 1000000;
        System.out.printf("Loading %d base vectors...%n", numBase);
        long t0 = System.currentTimeMillis();
        float[][] baseVectors = loadBaseVectors(numBase);
        System.out.printf("  Loaded in %.1f s%n", (System.currentTimeMillis() - t0) / 1000.0);

        System.out.printf("Loading %d query vectors...%n", NUM_QUERIES);
        float[][] queryVectors = loadQueryVectors(NUM_QUERIES);

        System.out.printf("Loading ground truth...%n");
        int[][] groundTruth = loadGroundTruth(NUM_QUERIES);
        System.out.println();

        // Build DiskBBQ index
        DataType vecType = new ArrayType(new FloatType());
        FileIO fileIO = new LocalFileIO();

        System.out.println("=== Building DiskBBQ Index ===");
        Path diskbbqPath = new Path(tempDir.toString(), "diskbbq");
        long buildStart = System.currentTimeMillis();
        List<ResultEntry> diskbbqEntries = buildDiskBBQIndex(baseVectors, vecType, diskbbqPath);
        long diskbbqBuildTime = System.currentTimeMillis() - buildStart;
        System.out.printf("  DiskBBQ build: %d ms, %d vectors%n%n", diskbbqBuildTime, numBase);

        List<GlobalIndexIOMeta> diskbbqMetas = toIOMetas(diskbbqEntries, diskbbqPath, fileIO);

        // Recall test: DiskBBQ serial
        System.out.println("=== Recall@" + TOP_K + " ===");
        Options serialOpts = createOptions(DIM, false);
        ESVectorIndexOptions serialIndexOpts = new ESVectorIndexOptions(serialOpts);
        double serialRecall =
                testRecall(
                        fileIO,
                        diskbbqPath,
                        diskbbqMetas,
                        vecType,
                        serialIndexOpts,
                        queryVectors,
                        groundTruth,
                        "DiskBBQ-Serial");

        // Recall test: DiskBBQ parallel
        Options parallelOpts = createOptions(DIM, true);
        ESVectorIndexOptions parallelIndexOpts = new ESVectorIndexOptions(parallelOpts);
        double parallelRecall =
                testRecall(
                        fileIO,
                        diskbbqPath,
                        diskbbqMetas,
                        vecType,
                        parallelIndexOpts,
                        queryVectors,
                        groundTruth,
                        "DiskBBQ-Parallel");

        // Recall test: DiskBBQ bulk
        Options bulkOpts = createOptions(DIM, false);
        bulkOpts.setString(ESVectorIndexOptions.SEARCH_BULK.key(), "true");
        ESVectorIndexOptions bulkIndexOpts = new ESVectorIndexOptions(bulkOpts);
        double bulkRecall =
                testRecall(
                        fileIO,
                        diskbbqPath,
                        diskbbqMetas,
                        vecType,
                        bulkIndexOpts,
                        queryVectors,
                        groundTruth,
                        "DiskBBQ-Bulk");

        System.out.println();

        // Latency test
        System.out.println("=== Latency (ms), top-" + TOP_K + " ===");
        double serialAvg =
                testLatency(
                        fileIO,
                        diskbbqPath,
                        diskbbqMetas,
                        vecType,
                        serialIndexOpts,
                        queryVectors,
                        "DiskBBQ-Serial");
        double parallelAvg =
                testLatency(
                        fileIO,
                        diskbbqPath,
                        diskbbqMetas,
                        vecType,
                        parallelIndexOpts,
                        queryVectors,
                        "DiskBBQ-Parallel");
        double bulkAvg =
                testLatency(
                        fileIO,
                        diskbbqPath,
                        diskbbqMetas,
                        vecType,
                        bulkIndexOpts,
                        queryVectors,
                        "DiskBBQ-Bulk");

        System.out.println();
        System.out.println("=== Summary ===");
        System.out.printf(
                "  DiskBBQ-Serial:   recall=%.1f%%, avg=%.1f ms%n", serialRecall * 100, serialAvg);
        System.out.printf(
                "  DiskBBQ-Parallel: recall=%.1f%%, avg=%.1f ms%n",
                parallelRecall * 100, parallelAvg);
        System.out.printf(
                "  DiskBBQ-Bulk:     recall=%.1f%%, avg=%.1f ms%n", bulkRecall * 100, bulkAvg);
        if (serialAvg > 0) {
            System.out.printf("  Parallel speedup: %.2fx%n", serialAvg / parallelAvg);
            System.out.printf("  Bulk speedup:     %.2fx%n", serialAvg / bulkAvg);
        }
        System.out.println("============================================================");
    }

    private List<ResultEntry> buildDiskBBQIndex(float[][] vectors, DataType vecType, Path indexPath)
            throws IOException {
        FileIO fileIO = new LocalFileIO();
        fileIO.mkdirs(indexPath);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath, fileIO);
        Options opts = createOptions(DIM, false);
        ESVectorIndexOptions indexOpts = new ESVectorIndexOptions(opts);
        ESVectorGlobalIndexWriter writer =
                new ESVectorGlobalIndexWriter(fileWriter, "v", vecType, indexOpts);
        for (float[] vec : vectors) {
            writer.write(vec);
        }
        return writer.finish();
    }

    private double testRecall(
            FileIO fileIO,
            Path indexPath,
            List<GlobalIndexIOMeta> metas,
            DataType vecType,
            ESVectorIndexOptions indexOpts,
            float[][] queryVectors,
            int[][] groundTruth,
            String label)
            throws IOException {
        GlobalIndexFileReader fileReader = createFileReader(indexPath, fileIO);
        double totalRecall = 0;
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vecType, indexOpts)) {
            for (int q = 0; q < queryVectors.length; q++) {
                VectorSearch vs = new VectorSearch(queryVectors[q], TOP_K, "v");
                ESScoredGlobalIndexResult result =
                        (ESScoredGlobalIndexResult) reader.visitVectorSearch(vs).orElse(null);
                if (result == null) {
                    continue;
                }
                Set<Long> resultIds = new HashSet<>();
                for (long id : result.results()) {
                    resultIds.add(id);
                }
                Set<Long> gtIds = new HashSet<>();
                for (int i = 0; i < Math.min(TOP_K, groundTruth[q].length); i++) {
                    gtIds.add((long) groundTruth[q][i]);
                }
                resultIds.retainAll(gtIds);
                totalRecall += (double) resultIds.size() / TOP_K;
            }
        }
        double avgRecall = totalRecall / queryVectors.length;
        System.out.printf("  %-18s recall@%d = %.3f%n", label + ":", TOP_K, avgRecall);
        return avgRecall;
    }

    private double testLatency(
            FileIO fileIO,
            Path indexPath,
            List<GlobalIndexIOMeta> metas,
            DataType vecType,
            ESVectorIndexOptions indexOpts,
            float[][] queryVectors,
            String label)
            throws IOException {
        GlobalIndexFileReader fileReader = createFileReader(indexPath, fileIO);
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vecType, indexOpts)) {
            // warmup
            for (int w = 0; w < WARMUP; w++) {
                VectorSearch vs =
                        new VectorSearch(queryVectors[w % queryVectors.length], TOP_K, "v");
                reader.visitVectorSearch(vs);
            }

            // bench
            long[] latencies = new long[BENCH_ROUNDS * queryVectors.length];
            int idx = 0;
            for (int r = 0; r < BENCH_ROUNDS; r++) {
                for (int q = 0; q < queryVectors.length; q++) {
                    VectorSearch vs = new VectorSearch(queryVectors[q], TOP_K, "v");
                    long start = System.nanoTime();
                    reader.visitVectorSearch(vs);
                    latencies[idx++] = System.nanoTime() - start;
                }
            }

            Arrays.sort(latencies);
            double avg = 0;
            for (long l : latencies) {
                avg += l;
            }
            avg = avg / latencies.length / 1e6;
            double p50 = latencies[latencies.length / 2] / 1e6;
            double p99 = latencies[(int) (latencies.length * 0.99)] / 1e6;
            double min = latencies[0] / 1e6;
            double max = latencies[latencies.length - 1] / 1e6;

            System.out.printf(
                    "  %-18s avg=%.2f ms, p50=%.2f ms, p99=%.2f ms, min=%.2f ms, max=%.2f ms%n",
                    label + ":", avg, p50, p99, min, max);
            return avg;
        }
    }

    // === Data loading ===

    private float[][] loadBaseVectors(int limit) throws IOException {
        float[][] vectors = new float[limit][DIM];
        try (BufferedReader br =
                new BufferedReader(new FileReader(SIFT_DIR + "/sift_base.fvecs.esrally"))) {
            for (int i = 0; i < limit; i++) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                vectors[i] = parseVectorFromJson(line);
            }
        }
        return vectors;
    }

    private float[][] loadQueryVectors(int limit) throws IOException {
        float[][] vectors = new float[limit][DIM];
        try (BufferedReader br =
                new BufferedReader(
                        new FileReader(SIFT_DIR + "/sift_query_esrally_operations.json"))) {
            for (int i = 0; i < limit; i++) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                // format: {"body":{"query":{"hnsw":{"vector":{"vector":[...], "size":100}}}}, ...}
                int vecStart = line.indexOf("\"vector\": [") + 11;
                if (vecStart < 11) {
                    vecStart = line.indexOf("\"vector\":[") + 10;
                }
                int vecEnd = line.indexOf("]", vecStart);
                String vecStr = line.substring(vecStart, vecEnd);
                String[] parts = vecStr.split(",");
                for (int d = 0; d < DIM; d++) {
                    vectors[i][d] = Float.parseFloat(parts[d].trim());
                }
            }
        }
        return vectors;
    }

    private int[][] loadGroundTruth(int limit) throws IOException {
        int[][] gt = new int[limit][];
        try (BufferedReader br =
                new BufferedReader(new FileReader(SIFT_DIR + "/sift_groundtruth.ivecs.json"))) {
            for (int i = 0; i < limit; i++) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                // format: {"matched_id": [id0, id1, ...], "query_id": N}
                int arrStart = line.indexOf("[") + 1;
                int arrEnd = line.indexOf("]");
                String[] parts = line.substring(arrStart, arrEnd).split(",");
                gt[i] = new int[parts.length];
                for (int j = 0; j < parts.length; j++) {
                    gt[i][j] = Integer.parseInt(parts[j].trim());
                }
            }
        }
        return gt;
    }

    private float[] parseVectorFromJson(String line) {
        // format: {"vector": [v0, v1, ...], "id": N}
        int arrStart = line.indexOf("[") + 1;
        int arrEnd = line.indexOf("]");
        String[] parts = line.substring(arrStart, arrEnd).split(",");
        float[] vec = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            vec[i] = Float.parseFloat(parts[i].trim());
        }
        return vec;
    }

    // === Helpers ===

    private Options createOptions(int dimension, boolean parallel) {
        Options options = new Options();
        options.setInteger(ESVectorIndexOptions.DIMENSION.key(), dimension);
        options.setString(ESVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
        if (parallel) {
            options.setString(ESVectorIndexOptions.SEARCH_PARALLEL.key(), "true");
        }
        return options;
    }

    private GlobalIndexFileWriter createFileWriter(Path path, FileIO fileIO) {
        return new GlobalIndexFileWriter() {
            @Override
            public String newFileName(String prefix) {
                return prefix + "-" + UUID.randomUUID();
            }

            @Override
            public PositionOutputStream newOutputStream(String fileName) throws IOException {
                return fileIO.newOutputStream(new Path(path, fileName), false);
            }
        };
    }

    private GlobalIndexFileReader createFileReader(Path path, FileIO fileIO) {
        return meta -> fileIO.newInputStream(new Path(path, meta.filePath()));
    }

    private List<GlobalIndexIOMeta> toIOMetas(List<ResultEntry> results, Path path, FileIO fileIO)
            throws IOException {
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (ResultEntry result : results) {
            Path filePath = new Path(path, result.fileName());
            metas.add(new GlobalIndexIOMeta(filePath, fileIO.getFileSize(filePath), result.meta()));
        }
        return metas;
    }
}
