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

import org.apache.paimon.elasticsearch.index.model.ESVectorIndexOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Rebuilds archive files with correct fieldName="v" to replace old archives that used "vector".
 * Output: /tmp/rebuild-archives/ with files named to match OSS paths.
 */
public class ArchiveRebuilder {

    private static final String OUTPUT_DIR = "/tmp/rebuild-archives";
    private static final int VECTORS_PER_SHARD = 20;
    private static final int DIMENSION = 128;
    private static final String FIELD_NAME = "v";
    private static final int NUM_CATEGORIES = 5;

    private static final String[] ARCHIVE_NAMES = {
        "es-archive-global-index-1d7697b6-1c25-4057-93e5-3be4b00434d6.index",
        "es-archive-global-index-46dbf851-9600-432b-bca5-79384f9c5f7f.index",
        "es-archive-global-index-4f71e8bc-8b0a-4ff1-bbdb-593c703d8e0a.index",
        "es-archive-global-index-5fc7ea6c-6b33-48d4-9a20-9a0e72a2825f.index",
        "es-archive-global-index-6e1c8583-204a-42dd-86d5-c8ef943bfa99.index",
        "es-archive-global-index-b5b7a5c5-f078-4d73-b1e6-d0141d147379.index",
        "es-archive-global-index-b9c98267-039f-4bdd-8356-c138e85ce489.index",
        "es-archive-global-index-ba05a406-55dd-4048-b487-02b8ec5dce73.index",
        "es-archive-global-index-e82054d7-6e31-4f2f-ba4e-c7a5955654ec.index",
        "es-archive-global-index-fd42164d-4b6c-44da-82a9-ca1295d971f5.index",
    };

    private final FileIO fileIO = new LocalFileIO();
    private final DataType vectorType = new ArrayType(new FloatType());

    @Test
    public void rebuildArchives() throws Exception {
        File outputRoot = new File(OUTPUT_DIR);
        if (outputRoot.exists()) {
            deleteRecursive(outputRoot);
        }
        outputRoot.mkdirs();

        Random random = new Random(42);

        for (int shard = 0; shard < ARCHIVE_NAMES.length; shard++) {
            String archiveName = ARCHIVE_NAMES[shard];
            Path shardPath = new Path(OUTPUT_DIR, "shard-" + shard);
            fileIO.mkdirs(shardPath);

            Options options = new Options();
            options.setInteger(ESVectorIndexOptions.DIMENSION.key(), DIMENSION);
            options.setString(ESVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
            ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);

            GlobalIndexFileWriter fileWriter = createFileWriter(shardPath, archiveName);
            ESVectorGlobalIndexWriter writer =
                    new ESVectorGlobalIndexWriter(fileWriter, FIELD_NAME, vectorType, indexOptions);

            List<float[]> vectors = generateClusteredVectors(VECTORS_PER_SHARD, random);
            for (float[] vec : vectors) {
                writer.write(vec);
            }

            List<ResultEntry> results = writer.finish();
            writer.close();

            // The archive file is now at shardPath/archiveName
            java.nio.file.Path archiveFile =
                    java.nio.file.Paths.get(shardPath.toString(), archiveName);
            java.nio.file.Path targetFile = java.nio.file.Paths.get(OUTPUT_DIR, archiveName);
            Files.copy(archiveFile, targetFile);

            System.out.printf(
                    "Shard %d: rebuilt archive %s (%d bytes, %d vectors, fieldName=%s)%n",
                    shard, archiveName, Files.size(targetFile), VECTORS_PER_SHARD, FIELD_NAME);
        }

        System.out.println("\nAll archives rebuilt at: " + OUTPUT_DIR);
        System.out.println("Upload command:");
        for (String name : ARCHIVE_NAMES) {
            System.out.printf(
                    "  ossutil64 cp %s/%s oss://fuchangfeng-test/paimon-warehouse/diskbbq_10w.db/test_diskbbq/index/%s -f%n",
                    OUTPUT_DIR, name, name);
        }
    }

    private GlobalIndexFileWriter createFileWriter(Path path, String archiveName) {
        return new GlobalIndexFileWriter() {
            @Override
            public String newFileName(String prefix) {
                if (prefix.equals("es-archive")) {
                    return archiveName;
                }
                return prefix + "-offsets.index";
            }

            @Override
            public PositionOutputStream newOutputStream(String fileName) throws IOException {
                return fileIO.newOutputStream(new Path(path, fileName), false);
            }
        };
    }

    private List<float[]> generateClusteredVectors(int count, Random random) {
        float[][] centroids = new float[NUM_CATEGORIES][DIMENSION];
        for (int c = 0; c < NUM_CATEGORIES; c++) {
            for (int d = 0; d < DIMENSION; d++) {
                centroids[c][d] = random.nextFloat() * 2 - 1;
            }
            normalize(centroids[c]);
        }

        List<float[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int category = i % NUM_CATEGORIES;
            float[] vec = new float[DIMENSION];
            for (int d = 0; d < DIMENSION; d++) {
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
