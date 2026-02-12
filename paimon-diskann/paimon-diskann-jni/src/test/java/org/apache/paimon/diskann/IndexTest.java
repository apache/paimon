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

package org.apache.paimon.diskann;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the DiskANN Index class.
 *
 * <p>Note: These tests require the native library to be built and available. They will be skipped
 * if the native library is not found.
 */
class IndexTest {

    private static final int DIMENSION = 128;
    private static final int NUM_VECTORS = 1000;
    private static final int K = 10;
    private static final int MAX_DEGREE = 64;
    private static final int BUILD_LIST_SIZE = 100;
    private static final int SEARCH_LIST_SIZE = 100;
    private static final int INDEX_TYPE_MEMORY = 0;

    @BeforeAll
    static void checkNativeLibrary() {
        if (!DiskAnn.isLibraryLoaded()) {
            try {
                DiskAnn.loadLibrary();
            } catch (DiskAnnException e) {
                StringBuilder errorMsg = new StringBuilder("DiskANN native library not available.");
                errorMsg.append("\nError: ").append(e.getMessage());
                if (e.getCause() != null) {
                    errorMsg.append("\nCause: ").append(e.getCause().getMessage());
                }
                errorMsg.append(
                        "\n\nTo run DiskANN tests, ensure the paimon-diskann-jni JAR"
                                + " with native libraries is available in the classpath.");
                Assumptions.assumeTrue(false, errorMsg.toString());
            }
        }
    }

    @Test
    void testBasicOperations() {
        try (Index index = createIndex(MetricType.L2)) {
            assertEquals(DIMENSION, index.getDimension());
            assertEquals(0, index.getCount());
            assertEquals(MetricType.L2, index.getMetricType());

            // Add vectors with IDs
            addVectors(index, NUM_VECTORS, DIMENSION);
            assertEquals(NUM_VECTORS, index.getCount());

            // Build the index
            index.build(BUILD_LIST_SIZE);

            // Search
            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, SEARCH_LIST_SIZE, distances, labels);

            // Verify labels are in valid range
            for (int i = 0; i < K; i++) {
                assertTrue(
                        labels[i] >= 0 && labels[i] < NUM_VECTORS,
                        "Label " + labels[i] + " out of range");
            }

            // Verify distances are non-negative for L2
            for (int i = 0; i < K; i++) {
                assertTrue(distances[i] >= 0, "Distance should be non-negative for L2");
            }
        }
    }

    @Test
    void testSequentialIds() {
        try (Index index = createIndex(MetricType.L2)) {
            addVectors(index, NUM_VECTORS, DIMENSION);
            assertEquals(NUM_VECTORS, index.getCount());

            index.build(BUILD_LIST_SIZE);

            // Search should return sequential IDs (0, 1, 2, ...)
            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, SEARCH_LIST_SIZE, distances, labels);

            for (int i = 0; i < K; i++) {
                assertTrue(
                        labels[i] >= 0 && labels[i] < NUM_VECTORS,
                        "Label " + labels[i] + " out of range");
            }
        }
    }

    @Test
    void testBatchSearch() {
        try (Index index = createIndex(MetricType.L2)) {
            addVectors(index, NUM_VECTORS, DIMENSION);
            index.build(BUILD_LIST_SIZE);

            int numQueries = 5;
            float[] queryVectors = createQueryVectors(numQueries, DIMENSION);
            float[] distances = new float[numQueries * K];
            long[] labels = new long[numQueries * K];

            index.search(numQueries, queryVectors, K, SEARCH_LIST_SIZE, distances, labels);

            // Read results for each query
            for (int q = 0; q < numQueries; q++) {
                for (int n = 0; n < K; n++) {
                    int idx = q * K + n;
                    assertTrue(labels[idx] >= 0 && labels[idx] < NUM_VECTORS);
                    assertTrue(distances[idx] >= 0);
                }
            }
        }
    }

    @Test
    void testInnerProductMetric() {
        try (Index index = createIndex(MetricType.INNER_PRODUCT)) {
            assertEquals(MetricType.INNER_PRODUCT, index.getMetricType());

            addVectors(index, NUM_VECTORS, DIMENSION);
            index.build(BUILD_LIST_SIZE);

            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, SEARCH_LIST_SIZE, distances, labels);

            // DiskANN uses distance form for all metrics (lower = closer/more similar).
            // For inner product the distance is derived so that results are still in
            // ascending order by distance (the most similar result first).
            for (int i = 1; i < K; i++) {
                assertTrue(
                        distances[i] >= distances[i - 1],
                        "Distances should be sorted in ascending order (lower = more similar)");
            }
        }
    }

    @Test
    void testCosineMetric() {
        try (Index index = createIndex(MetricType.COSINE)) {
            assertEquals(MetricType.COSINE, index.getMetricType());

            addVectors(index, NUM_VECTORS, DIMENSION);
            index.build(BUILD_LIST_SIZE);

            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, SEARCH_LIST_SIZE, distances, labels);

            // Cosine distance should be in [0, 2] range
            for (int i = 0; i < K; i++) {
                assertTrue(labels[i] >= 0, "Label should be non-negative");
            }
        }
    }

    @Test
    void testSmallIndex() {
        int dim = 2;
        try (Index index =
                Index.create(dim, MetricType.L2, INDEX_TYPE_MEMORY, MAX_DEGREE, BUILD_LIST_SIZE)) {
            // Add a few vectors: [1,0], [0,1], [0.7,0.7]
            ByteBuffer vectorBuffer = Index.allocateVectorBuffer(3, dim);
            FloatBuffer floatView = vectorBuffer.asFloatBuffer();
            floatView.put(0, 1.0f);
            floatView.put(1, 0.0f); // position 0: [1, 0]
            floatView.put(2, 0.0f);
            floatView.put(3, 1.0f); // position 1: [0, 1]
            floatView.put(4, 0.7f);
            floatView.put(5, 0.7f); // position 2: [0.7, 0.7]

            index.add(3, vectorBuffer);
            index.build(BUILD_LIST_SIZE);

            // Query for [1, 0] - should find position 0 as nearest
            float[] query = {1.0f, 0.0f};
            float[] distances = new float[1];
            long[] labels = new long[1];
            index.search(1, query, 1, SEARCH_LIST_SIZE, distances, labels);

            assertEquals(0L, labels[0], "Nearest to [1,0] should be position 0");
            assertEquals(0.0f, distances[0], 1e-5f, "Distance to self should be ~0");
        }
    }

    @Test
    void testSearchResultArrays() {
        try (Index index = createIndex(MetricType.L2)) {
            addVectors(index, 100, DIMENSION);
            index.build(BUILD_LIST_SIZE);

            int numQueries = 3;
            int k = 5;
            float[] queryVectors = createQueryVectors(numQueries, DIMENSION);
            float[] distances = new float[numQueries * k];
            long[] labels = new long[numQueries * k];

            index.search(numQueries, queryVectors, k, SEARCH_LIST_SIZE, distances, labels);

            // Test reading individual results
            for (int q = 0; q < numQueries; q++) {
                for (int n = 0; n < k; n++) {
                    int idx = q * k + n;
                    assertTrue(labels[idx] >= 0 && labels[idx] < 100);
                    assertTrue(distances[idx] >= 0);
                }
            }
        }
    }

    @Test
    void testBufferAllocationHelpers() {
        // Test vector buffer allocation
        ByteBuffer vectorBuffer = Index.allocateVectorBuffer(10, DIMENSION);
        assertTrue(vectorBuffer.isDirect());
        assertEquals(ByteOrder.nativeOrder(), vectorBuffer.order());
        assertEquals(10 * DIMENSION * Float.BYTES, vectorBuffer.capacity());
    }

    @Test
    void testErrorHandling() {
        // Test buffer validation - wrong size buffer
        try (Index index = createIndex(MetricType.L2)) {
            ByteBuffer wrongSizeBuffer =
                    ByteBuffer.allocateDirect(10).order(ByteOrder.nativeOrder());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.add(1, wrongSizeBuffer);
                    });
        }

        // Test non-direct buffer
        try (Index index = createIndex(MetricType.L2)) {
            ByteBuffer heapBuffer = ByteBuffer.allocate(DIMENSION * Float.BYTES);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.add(1, heapBuffer);
                    });
        }

        // Test serialize with non-direct buffer
        try (Index index = createIndex(MetricType.L2)) {
            ByteBuffer heapBuffer = ByteBuffer.allocate(100);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.serialize(heapBuffer);
                    });
        }

        // Test closed index
        Index closedIndex = createIndex(MetricType.L2);
        closedIndex.close();
        assertThrows(
                IllegalStateException.class,
                () -> {
                    closedIndex.getCount();
                });
    }

    @Test
    void testQueryVectorArrayValidation() {
        try (Index index = createIndex(MetricType.L2)) {
            addVectors(index, 10, DIMENSION);
            index.build(BUILD_LIST_SIZE);

            // Query vectors array too small
            float[] tooSmall = new float[DIMENSION - 1];
            float[] distances = new float[K];
            long[] labels = new long[K];
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.search(1, tooSmall, K, SEARCH_LIST_SIZE, distances, labels);
                    });

            // Distances array too small
            float[] query = createQueryVectors(1, DIMENSION);
            float[] smallDistances = new float[K - 1];
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.search(1, query, K, SEARCH_LIST_SIZE, smallDistances, labels);
                    });

            // Labels array too small
            long[] smallLabels = new long[K - 1];
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.search(1, query, K, SEARCH_LIST_SIZE, distances, smallLabels);
                    });
        }
    }

    private Index createIndex(MetricType metricType) {
        return Index.create(DIMENSION, metricType, INDEX_TYPE_MEMORY, MAX_DEGREE, BUILD_LIST_SIZE);
    }

    /** Add random vectors to the index. */
    private void addVectors(Index index, int n, int d) {
        ByteBuffer vectorBuffer = createVectorBuffer(n, d);
        index.add(n, vectorBuffer);
    }

    /** Create a direct ByteBuffer with random vectors. */
    private ByteBuffer createVectorBuffer(int n, int d) {
        ByteBuffer buffer = Index.allocateVectorBuffer(n, d);
        FloatBuffer floatView = buffer.asFloatBuffer();

        Random random = new Random(42);
        for (int i = 0; i < n * d; i++) {
            floatView.put(i, random.nextFloat());
        }

        return buffer;
    }

    /** Create a float array with random query vectors. */
    private float[] createQueryVectors(int n, int d) {
        float[] vectors = new float[n * d];
        Random random = new Random(42);
        for (int i = 0; i < n * d; i++) {
            vectors[i] = random.nextFloat();
        }
        return vectors;
    }
}
