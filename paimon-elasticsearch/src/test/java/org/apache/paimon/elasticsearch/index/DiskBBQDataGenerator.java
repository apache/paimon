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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates 200 DiskBBQ test vectors across 5 shards and writes to /tmp/diskbbq-test-data/. Each
 * shard contains 40 vectors (128-dim, L2 metric).
 */
public class DiskBBQDataGenerator {

    private static final String OUTPUT_DIR = "/tmp/diskbbq-test-data";
    private static final int TOTAL_VECTORS = 200;
    private static final int NUM_SHARDS = 5;
    private static final int VECTORS_PER_SHARD = TOTAL_VECTORS / NUM_SHARDS;
    private static final int DIMENSION = 128;
    private static final String METRIC = "l2";
    private static final int NUM_CATEGORIES = 5;

    private final FileIO fileIO = new LocalFileIO();
    private final DataType vectorType = new ArrayType(new FloatType());

    @Test
    public void generateTestData() throws Exception {
        File outputRoot = new File(OUTPUT_DIR);
        if (outputRoot.exists()) {
            deleteRecursive(outputRoot);
        }
        outputRoot.mkdirs();

        Random random = new Random(42);
        List<float[]> allVectors =
                generateClusteredVectors(TOTAL_VECTORS, DIMENSION, NUM_CATEGORIES, random);

        StringBuilder manifest = new StringBuilder();
        manifest.append("# DiskBBQ Test Data Manifest\n\n");
        manifest.append(String.format("- total_vectors: %d\n", TOTAL_VECTORS));
        manifest.append(String.format("- num_shards: %d\n", NUM_SHARDS));
        manifest.append(String.format("- vectors_per_shard: %d\n", VECTORS_PER_SHARD));
        manifest.append(String.format("- dimension: %d\n", DIMENSION));
        manifest.append(String.format("- metric: %s\n", METRIC));
        manifest.append(String.format("- categories: %d\n\n", NUM_CATEGORIES));
        manifest.append("## Shards\n\n");

        for (int shard = 0; shard < NUM_SHARDS; shard++) {
            String shardDir = String.format("shard-%d", shard);
            Path shardPath = new Path(OUTPUT_DIR, shardDir);
            fileIO.mkdirs(shardPath);

            int startIdx = shard * VECTORS_PER_SHARD;
            int endIdx = startIdx + VECTORS_PER_SHARD;
            List<float[]> shardVectors = allVectors.subList(startIdx, endIdx);

            Options options = new Options();
            options.setInteger(ESVectorIndexOptions.DIMENSION.key(), DIMENSION);
            options.setString(ESVectorIndexOptions.DISTANCE_METRIC.key(), METRIC);
            ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);

            GlobalIndexFileWriter fileWriter = createFileWriter(shardPath);
            ESVectorGlobalIndexWriter writer =
                    new ESVectorGlobalIndexWriter(fileWriter, "vec", vectorType, indexOptions);

            for (float[] vec : shardVectors) {
                writer.write(vec);
            }
            List<ResultEntry> results = writer.finish();
            writer.close();

            ResultEntry entry = results.get(0);
            Path indexFilePath = new Path(shardPath, entry.fileName());
            long fileSize = fileIO.getFileSize(indexFilePath);

            String metaBase64 = Base64.getEncoder().encodeToString(entry.meta());
            StringBuilder shardMeta = new StringBuilder();
            shardMeta.append(String.format("file=%s\n", entry.fileName()));
            shardMeta.append(String.format("count=%d\n", entry.rowCount()));
            shardMeta.append(String.format("fileSize=%d\n", fileSize));
            shardMeta.append(String.format("meta=%s\n", metaBase64));
            Files.write(
                    new File(OUTPUT_DIR + "/" + shardDir + "/shard-meta.properties").toPath(),
                    shardMeta.toString().getBytes(StandardCharsets.UTF_8));

            manifest.append(String.format("### shard-%d\n", shard));
            manifest.append(
                    String.format(
                            "- vectors: %d (global row %d-%d)\n",
                            VECTORS_PER_SHARD, startIdx, endIdx - 1));
            manifest.append(String.format("- index_file: %s\n", entry.fileName()));
            manifest.append(String.format("- file_size: %d bytes\n", fileSize));
            manifest.append(
                    String.format(
                            "- categories: %s\n\n",
                            describeCategoryDistribution(startIdx, endIdx)));

            // verify shard: read back and search
            GlobalIndexFileReader fileReader =
                    meta -> fileIO.newInputStream(new Path(shardPath, meta.filePath()));
            GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(indexFilePath, fileSize, entry.meta());
            try (ESVectorGlobalIndexReader reader =
                    new ESVectorGlobalIndexReader(
                            fileReader,
                            Collections.singletonList(ioMeta),
                            vectorType,
                            indexOptions)) {
                VectorSearch search = new VectorSearch(shardVectors.get(0), 5, "vec");
                ESScoredGlobalIndexResult result =
                        (ESScoredGlobalIndexResult) reader.visitVectorSearch(search).get();
                manifest.append(
                        String.format(
                                "- verify_search: top5 returned %d results, contains row 0: %s\n\n",
                                result.results().getLongCardinality(),
                                result.results().contains(0L)));
            }

            System.out.printf(
                    "Shard %d: wrote %d vectors, file=%s (%d bytes)%n",
                    shard, VECTORS_PER_SHARD, entry.fileName(), fileSize);
        }

        Files.write(
                new File(OUTPUT_DIR + "/manifest.md").toPath(),
                manifest.toString().getBytes(StandardCharsets.UTF_8));

        System.out.println("\nAll shards generated at: " + OUTPUT_DIR);
        System.out.println("Manifest written to: " + OUTPUT_DIR + "/manifest.md");
    }

    private List<float[]> generateClusteredVectors(
            int total, int dim, int numCategories, Random random) {
        List<float[]> vectors = new ArrayList<>(total);
        float[][] centroids = new float[numCategories][dim];
        for (int c = 0; c < numCategories; c++) {
            for (int d = 0; d < dim; d++) {
                centroids[c][d] = random.nextFloat() * 2 - 1;
            }
            normalize(centroids[c]);
        }

        for (int i = 0; i < total; i++) {
            int category = i % numCategories;
            float[] vec = new float[dim];
            for (int d = 0; d < dim; d++) {
                vec[d] = centroids[category][d] + (random.nextFloat() - 0.5f) * 0.3f;
            }
            normalize(vec);
            vectors.add(vec);
        }
        return vectors;
    }

    private void normalize(float[] vec) {
        float norm = 0;
        for (float v : vec) {
            norm += v * v;
        }
        norm = (float) Math.sqrt(norm);
        if (norm > 0) {
            for (int i = 0; i < vec.length; i++) {
                vec[i] /= norm;
            }
        }
    }

    private String describeCategoryDistribution(int startIdx, int endIdx) {
        int[] counts = new int[NUM_CATEGORIES];
        for (int i = startIdx; i < endIdx; i++) {
            counts[i % NUM_CATEGORIES]++;
        }
        StringBuilder sb = new StringBuilder();
        for (int c = 0; c < NUM_CATEGORIES; c++) {
            if (c > 0) {
                sb.append(", ");
            }
            sb.append(String.format("cat%d=%d", c, counts[c]));
        }
        return sb.toString();
    }

    private GlobalIndexFileWriter createFileWriter(Path path) {
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

    private void deleteRecursive(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursive(child);
                }
            }
        }
        file.delete();
    }
}
