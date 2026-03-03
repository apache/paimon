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

package org.apache.paimon.lumina;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests that verify the Lumina JNI layer works end-to-end: build → dump → open → search. */
class LuminaJniTest {

    private static final int DIM = 4;
    private static final int NUM_VECTORS = 100;
    private static final String INDEX_TYPE = "diskann";

    @TempDir java.nio.file.Path tempDir;

    @BeforeAll
    static void loadLibrary() {
        if (!Lumina.isLibraryLoaded()) {
            try {
                Lumina.loadLibrary();
            } catch (Throwable e) {
                String msg =
                        "Lumina native library not available: "
                                + e.getMessage()
                                + (e.getCause() != null
                                        ? "\nCause: " + e.getCause().getMessage()
                                        : "")
                                + "\njava.library.path="
                                + System.getProperty("java.library.path")
                                + "\nLD_LIBRARY_PATH="
                                + System.getenv("LD_LIBRARY_PATH");
                System.err.println(msg);
                Assumptions.assumeTrue(false, msg);
            }
        }
    }

    @Test
    void testBuildAndSearch() {
        File indexFile = new File(tempDir.toFile(), "test.idx");

        try (LuminaBuilder builder =
                LuminaBuilder.create(INDEX_TYPE, DIM, MetricType.L2)) {

            ByteBuffer vectors = LuminaBuilder.allocateVectorBuffer(NUM_VECTORS, DIM);
            ByteBuffer ids = LuminaBuilder.allocateIdBuffer(NUM_VECTORS);

            for (int i = 0; i < NUM_VECTORS; i++) {
                for (int d = 0; d < DIM; d++) {
                    vectors.putFloat(i * DIM * Float.BYTES + d * Float.BYTES, (float) i + d);
                }
                ids.putLong(i * Long.BYTES, i);
            }

            builder.pretrain(vectors, NUM_VECTORS);
            builder.insertBatch(vectors, ids, NUM_VECTORS);
            builder.dump(indexFile.getAbsolutePath());
        }

        assertTrue(indexFile.exists(), "Index file should be created");
        assertTrue(indexFile.length() > 0, "Index file should not be empty");

        try (LuminaSearcher searcher =
                LuminaSearcher.create(INDEX_TYPE, DIM, MetricType.L2)) {

            searcher.open(indexFile);

            assertEquals(NUM_VECTORS, searcher.getCount());
            assertEquals(DIM, searcher.getDimension());

            int topk = 5;
            float[] query = new float[DIM];
            for (int d = 0; d < DIM; d++) {
                query[d] = 0.0f + d;
            }

            float[] distances = new float[topk];
            long[] resultIds = new long[topk];

            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("list_size", String.valueOf(Math.max(topk, 100)));
            searcher.search(1, query, topk, distances, resultIds, opts);

            assertEquals(0, resultIds[0], "Nearest neighbor of vector 0 should be id 0");

            for (int i = 1; i < topk; i++) {
                assertTrue(
                        distances[i] >= distances[i - 1],
                        "Distances should be non-decreasing");
            }
        }
    }

    @Test
    void testSearchWithDifferentMetrics() {
        for (MetricType metric : new MetricType[] {MetricType.L2, MetricType.COSINE}) {
            File indexFile = new File(tempDir.toFile(), "test_" + metric.getLuminaValue() + ".idx");

            try (LuminaBuilder builder =
                    LuminaBuilder.create(INDEX_TYPE, DIM, metric)) {

                ByteBuffer vectors = LuminaBuilder.allocateVectorBuffer(NUM_VECTORS, DIM);
                ByteBuffer ids = LuminaBuilder.allocateIdBuffer(NUM_VECTORS);

                for (int i = 0; i < NUM_VECTORS; i++) {
                    for (int d = 0; d < DIM; d++) {
                        vectors.putFloat(
                                i * DIM * Float.BYTES + d * Float.BYTES, (float) (i + 1) + d);
                    }
                    ids.putLong(i * Long.BYTES, i);
                }

                builder.pretrain(vectors, NUM_VECTORS);
                builder.insertBatch(vectors, ids, NUM_VECTORS);
                builder.dump(indexFile.getAbsolutePath());
            }

            try (LuminaSearcher searcher =
                    LuminaSearcher.create(INDEX_TYPE, DIM, metric)) {

                searcher.open(indexFile);
                assertEquals(NUM_VECTORS, searcher.getCount());

                float[] query = new float[DIM];
                for (int d = 0; d < DIM; d++) {
                    query[d] = 1.0f + d;
                }

                float[] distances = new float[3];
                long[] resultIds = new long[3];

                Map<String, String> opts = Collections.singletonMap("list_size", "100");
                searcher.search(1, query, 3, distances, resultIds, opts);

                assertTrue(resultIds.length > 0, metric + ": should return results");
            }
        }
    }

    @Test
    void testBuilderCloseIsIdempotent() {
        LuminaBuilder builder = LuminaBuilder.create(INDEX_TYPE, DIM, MetricType.L2);
        builder.close();
        builder.close();
    }

    @Test
    void testSearcherCloseIsIdempotent() {
        LuminaSearcher searcher = LuminaSearcher.create(INDEX_TYPE, DIM, MetricType.L2);
        searcher.close();
        searcher.close();
    }
}
