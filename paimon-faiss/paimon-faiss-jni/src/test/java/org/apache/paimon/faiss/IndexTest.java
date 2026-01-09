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

package org.apache.paimon.faiss;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the Faiss Index class.
 *
 * <p>Note: These tests require the native library to be built and available. They will be skipped
 * if the native library is not found.
 */
class IndexTest {

    private static final int DIMENSION = 128;
    private static final int NUM_VECTORS = 1000;
    private static final int K = 10;

    @Test
    void testFlatIndexBasicOperations() {
        try (Index index = createFlatIndexWithMetric(MetricType.L2)) {
            assertEquals(DIMENSION, index.getDimension());
            assertEquals(0, index.getCount());
            assertTrue(index.isTrained());
            assertEquals(MetricType.L2, index.getMetricType());

            // Add vectors using zero-copy API
            ByteBuffer vectorBuffer = createVectorBuffer(NUM_VECTORS, DIMENSION);
            index.add(NUM_VECTORS, vectorBuffer);
            assertEquals(NUM_VECTORS, index.getCount());

            // Search using array API
            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, distances, labels);

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
    void testFlatIndexWithIds() {
        try (Index index = IndexFactory.create(DIMENSION, "IDMap,Flat", MetricType.L2)) {
            ByteBuffer vectorBuffer = createVectorBuffer(NUM_VECTORS, DIMENSION);
            ByteBuffer idBuffer = Index.allocateIdBuffer(NUM_VECTORS);
            idBuffer.asLongBuffer();
            for (int i = 0; i < NUM_VECTORS; i++) {
                idBuffer.putLong(i * Long.BYTES, i * 100L); // Use custom IDs
            }

            index.addWithIds(NUM_VECTORS, vectorBuffer, idBuffer);
            assertEquals(NUM_VECTORS, index.getCount());

            // Search should return our custom IDs
            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, distances, labels);

            for (int i = 0; i < K; i++) {
                assertTrue(labels[i] % 100 == 0, "Label should be a multiple of 100");
            }
        }
    }

    @Test
    void testBatchSearch() {
        try (Index index = createFlatIndexWithMetric(MetricType.L2)) {
            ByteBuffer vectorBuffer = createVectorBuffer(NUM_VECTORS, DIMENSION);
            index.add(NUM_VECTORS, vectorBuffer);

            int numQueries = 5;
            float[] queryVectors = createQueryVectors(numQueries, DIMENSION);
            float[] distances = new float[numQueries * K];
            long[] labels = new long[numQueries * K];

            index.search(numQueries, queryVectors, K, distances, labels);

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
        try (Index index = createFlatIndexWithMetric(MetricType.INNER_PRODUCT)) {
            assertEquals(MetricType.INNER_PRODUCT, index.getMetricType());

            ByteBuffer vectorBuffer = createVectorBuffer(NUM_VECTORS, DIMENSION);
            index.add(NUM_VECTORS, vectorBuffer);

            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, distances, labels);

            // For inner product, higher is better, so first result should have highest score
            for (int i = 1; i < K; i++) {
                assertTrue(
                        distances[i - 1] >= distances[i],
                        "Distances should be sorted in descending order for inner product");
            }
        }
    }

    @Test
    void testIndexReset() {
        try (Index index = createFlatIndex()) {
            ByteBuffer vectorBuffer = createVectorBuffer(100, DIMENSION);
            index.add(100, vectorBuffer);
            assertEquals(100, index.getCount());

            index.reset();
            assertEquals(0, index.getCount());

            // Can add again after reset
            index.add(100, vectorBuffer);
            assertEquals(100, index.getCount());
        }
    }

    @Test
    void testIndexSerialization(@TempDir Path tempDir) {
        ByteBuffer vectorBuffer = createVectorBuffer(NUM_VECTORS, DIMENSION);
        float[] queryVectors = createQueryVectors(1, DIMENSION);
        long[] originalLabels = new long[K];
        float[] originalDistances = new float[K];

        // Create and populate index
        try (Index index = createFlatIndex()) {
            index.add(NUM_VECTORS, vectorBuffer);

            // Perform search and save original results
            index.search(1, queryVectors, K, originalDistances, originalLabels);

            // Test file I/O
            File indexFile = tempDir.resolve("test.index").toFile();
            index.writeToFile(indexFile);

            try (Index loadedIndex = Index.readFromFile(indexFile)) {
                assertEquals(DIMENSION, loadedIndex.getDimension());
                assertEquals(NUM_VECTORS, loadedIndex.getCount());

                float[] loadedDistances = new float[K];
                long[] loadedLabels = new long[K];
                loadedIndex.search(1, queryVectors, K, loadedDistances, loadedLabels);

                assertArrayEquals(originalLabels, loadedLabels);
            }
        }

        // Test ByteBuffer serialization (zero-copy for write) and byte[] deserialization
        try (Index index = createFlatIndex()) {
            index.add(NUM_VECTORS, vectorBuffer);

            long serializeSize = index.serializeSize();
            assertTrue(serializeSize > 0);

            ByteBuffer serialized =
                    ByteBuffer.allocateDirect((int) serializeSize).order(ByteOrder.nativeOrder());
            long bytesWritten = index.serialize(serialized);
            assertEquals(serializeSize, bytesWritten);

            // Convert to byte array for deserialization
            serialized.rewind();
            byte[] serializedBytes = new byte[(int) bytesWritten];
            serialized.get(serializedBytes);

            try (Index deserializedIndex = Index.deserialize(serializedBytes)) {
                assertEquals(DIMENSION, deserializedIndex.getDimension());
                assertEquals(NUM_VECTORS, deserializedIndex.getCount());

                float[] deserializedDistances = new float[K];
                long[] deserializedLabels = new long[K];
                deserializedIndex.search(
                        1, queryVectors, K, deserializedDistances, deserializedLabels);

                assertArrayEquals(originalLabels, deserializedLabels);
            }
        }
    }

    @Test
    void testIndexFactoryDescriptions() {
        // Test various index factory strings
        String[] descriptions = {"Flat", "IDMap,Flat", "HNSW32", "HNSW32,Flat"};

        for (String desc : descriptions) {
            try (Index index = IndexFactory.create(DIMENSION, desc, MetricType.L2)) {
                assertEquals(DIMENSION, index.getDimension());
                assertNotNull(index.toString());
            }
        }
    }

    @Test
    void testHNSWIndex() {
        try (Index index = IndexFactory.create(DIMENSION, "HNSW" + 32, MetricType.L2)) {
            assertTrue(index.isTrained()); // HNSW doesn't need training

            ByteBuffer vectorBuffer = createVectorBuffer(NUM_VECTORS, DIMENSION);
            index.add(NUM_VECTORS, vectorBuffer);

            // Get and set efSearch
            int efSearch = IndexHNSW.getEfSearch(index);
            assertTrue(efSearch > 0);

            IndexHNSW.setEfSearch(index, 64);
            assertEquals(64, IndexHNSW.getEfSearch(index));

            // Search
            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, distances, labels);

            for (int i = 0; i < K; i++) {
                assertTrue(labels[i] >= 0);
            }
        }
    }

    @Test
    void testIVFSQ8Index() {
        // IVF16384,SQ8 is a quantized index that needs training
        try (Index index = IndexFactory.create(DIMENSION, "IVF16384,SQ8", MetricType.L2)) {
            assertEquals(DIMENSION, index.getDimension());
            assertEquals(MetricType.L2, index.getMetricType());

            // IVF index needs training
            assertTrue(!index.isTrained(), "IVF index should not be trained initially");

            // Train the index with training vectors
            int numTrainingVectors = 20000; // Should be >= nlist (16384) for good training
            ByteBuffer trainingBuffer = createVectorBuffer(numTrainingVectors, DIMENSION);
            index.train(numTrainingVectors, trainingBuffer);

            assertTrue(index.isTrained(), "Index should be trained after training");

            // Add vectors after training
            ByteBuffer vectorBuffer = createVectorBuffer(NUM_VECTORS, DIMENSION);
            index.add(NUM_VECTORS, vectorBuffer);
            assertEquals(NUM_VECTORS, index.getCount());

            // Set nprobe for search (number of clusters to visit)
            IndexIVF.setNprobe(index, 64);
            assertEquals(64, IndexIVF.getNprobe(index));

            // Search
            float[] queryVectors = createQueryVectors(1, DIMENSION);
            float[] distances = new float[K];
            long[] labels = new long[K];

            index.search(1, queryVectors, K, distances, labels);

            // Verify search results
            for (int i = 0; i < K; i++) {
                assertTrue(
                        labels[i] >= 0 && labels[i] < NUM_VECTORS,
                        "Label " + labels[i] + " out of range");
                assertTrue(distances[i] >= 0, "Distance should be non-negative for L2");
            }

            // Test batch search
            int numQueries = 3;
            float[] batchQueryVectors = createQueryVectors(numQueries, DIMENSION);
            float[] batchDistances = new float[numQueries * K];
            long[] batchLabels = new long[numQueries * K];

            index.search(numQueries, batchQueryVectors, K, batchDistances, batchLabels);

            for (int q = 0; q < numQueries; q++) {
                for (int n = 0; n < K; n++) {
                    int idx = q * K + n;
                    assertTrue(batchLabels[idx] >= 0 && batchLabels[idx] < NUM_VECTORS);
                    assertTrue(batchDistances[idx] >= 0);
                }
            }
        }
    }

    @Test
    void testErrorHandling() {
        // Test invalid dimension
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    IndexFactory.create(0, "Flat", MetricType.L2);
                });

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    IndexFactory.create(-1, "Flat", MetricType.L2);
                });

        // Test null description
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    IndexFactory.create(DIMENSION, null, MetricType.L2);
                });

        // Test buffer validation - wrong size buffer
        try (Index index = createFlatIndex()) {
            ByteBuffer wrongSizeBuffer =
                    ByteBuffer.allocateDirect(10).order(ByteOrder.nativeOrder());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.add(1, wrongSizeBuffer); // Buffer too small for 1 vector
                    });
        }

        // Test non-direct buffer
        try (Index index = createFlatIndex()) {
            ByteBuffer heapBuffer = ByteBuffer.allocate(DIMENSION * Float.BYTES);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.add(1, heapBuffer); // Not a direct buffer
                    });
        }

        // Test closed index
        Index closedIndex = createFlatIndex();
        closedIndex.close();
        assertThrows(
                IllegalStateException.class,
                () -> {
                    closedIndex.getCount();
                });
    }

    @Test
    void testSearchResultArrays() {
        try (Index index = createFlatIndex()) {
            ByteBuffer vectorBuffer = createVectorBuffer(100, DIMENSION);
            index.add(100, vectorBuffer);

            int numQueries = 3;
            int k = 5;
            float[] queryVectors = createQueryVectors(numQueries, DIMENSION);
            float[] distances = new float[numQueries * k];
            long[] labels = new long[numQueries * k];

            index.search(numQueries, queryVectors, k, distances, labels);

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

        // Test ID buffer allocation
        ByteBuffer idBuffer = Index.allocateIdBuffer(10);
        assertTrue(idBuffer.isDirect());
        assertEquals(ByteOrder.nativeOrder(), idBuffer.order());
        assertEquals(10 * Long.BYTES, idBuffer.capacity());
    }

    private Index createFlatIndexWithMetric(MetricType metricType) {
        return IndexFactory.create(DIMENSION, "Flat", metricType);
    }

    private Index createFlatIndex() {
        return IndexFactory.create(DIMENSION, "Flat", MetricType.L2);
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
