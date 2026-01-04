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
        try (Index index = IndexFactory.createFlat(DIMENSION, MetricType.L2)) {
            assertEquals(DIMENSION, index.getDimension());
            assertEquals(0, index.getCount());
            assertTrue(index.isTrained());
            assertEquals(MetricType.L2, index.getMetricType());

            // Add vectors
            float[] vectors = generateRandomVectors(NUM_VECTORS, DIMENSION);
            index.add(vectors);
            assertEquals(NUM_VECTORS, index.getCount());

            // Search
            float[] query = generateRandomVectors(1, DIMENSION);
            SearchResult result = index.searchSingle(query, K);

            assertEquals(1, result.getNumQueries());
            assertEquals(K, result.getK());
            assertEquals(K, result.getLabelsForQuery(0).length);
            assertEquals(K, result.getDistancesForQuery(0).length);

            // Verify labels are in valid range
            for (long label : result.getLabels()) {
                assertTrue(label >= 0 && label < NUM_VECTORS, "Label " + label + " out of range");
            }

            // Verify distances are non-negative for L2
            for (float distance : result.getDistances()) {
                assertTrue(distance >= 0, "Distance should be non-negative for L2");
            }
        }
    }

    @Test
    void testFlatIndexWithIds() {
        try (Index index = IndexFactory.createFlatWithIds(DIMENSION, MetricType.L2)) {
            float[] vectors = generateRandomVectors(NUM_VECTORS, DIMENSION);
            long[] ids = new long[NUM_VECTORS];
            for (int i = 0; i < NUM_VECTORS; i++) {
                ids[i] = i * 100; // Use custom IDs
            }

            index.addWithIds(vectors, ids);
            assertEquals(NUM_VECTORS, index.getCount());

            // Search should return our custom IDs
            float[] query = generateRandomVectors(1, DIMENSION);
            SearchResult result = index.searchSingle(query, K);

            for (long label : result.getLabels()) {
                assertTrue(label % 100 == 0, "Label should be a multiple of 100");
            }
        }
    }

    @Test
    void testBatchSearch() {
        try (Index index = IndexFactory.createFlat(DIMENSION)) {
            float[] vectors = generateRandomVectors(NUM_VECTORS, DIMENSION);
            index.add(vectors);

            int numQueries = 5;
            float[] queries = generateRandomVectors(numQueries, DIMENSION);
            SearchResult result = index.search(queries, K);

            assertEquals(numQueries, result.getNumQueries());
            assertEquals(K, result.getK());
            assertEquals(numQueries * K, result.getLabels().length);
            assertEquals(numQueries * K, result.getDistances().length);

            // Test per-query accessors
            for (int q = 0; q < numQueries; q++) {
                long[] labels = result.getLabelsForQuery(q);
                float[] distances = result.getDistancesForQuery(q);
                assertEquals(K, labels.length);
                assertEquals(K, distances.length);
            }
        }
    }

    @Test
    void testInnerProductMetric() {
        try (Index index = IndexFactory.createFlat(DIMENSION, MetricType.INNER_PRODUCT)) {
            assertEquals(MetricType.INNER_PRODUCT, index.getMetricType());

            float[] vectors = generateRandomVectors(NUM_VECTORS, DIMENSION);
            index.add(vectors);

            float[] query = generateRandomVectors(1, DIMENSION);
            SearchResult result = index.searchSingle(query, K);

            // For inner product, higher is better, so first result should have highest score
            float[] distances = result.getDistancesForQuery(0);
            for (int i = 1; i < K; i++) {
                assertTrue(
                        distances[i - 1] >= distances[i],
                        "Distances should be sorted in descending order for inner product");
            }
        }
    }

    @Test
    void testIndexReset() {
        try (Index index = IndexFactory.createFlat(DIMENSION)) {
            float[] vectors = generateRandomVectors(100, DIMENSION);
            index.add(vectors);
            assertEquals(100, index.getCount());

            index.reset();
            assertEquals(0, index.getCount());

            // Can add again after reset
            index.add(vectors);
            assertEquals(100, index.getCount());
        }
    }

    @Test
    void testIndexSerialization(@TempDir Path tempDir) {
        float[] vectors = generateRandomVectors(NUM_VECTORS, DIMENSION);
        float[] query = generateRandomVectors(1, DIMENSION);
        SearchResult originalResult;

        // Create and populate index
        try (Index index = IndexFactory.createFlat(DIMENSION)) {
            index.add(vectors);
            originalResult = index.searchSingle(query, K);

            // Test file I/O
            File indexFile = tempDir.resolve("test.index").toFile();
            index.writeToFile(indexFile);

            try (Index loadedIndex = Index.readFromFile(indexFile)) {
                assertEquals(DIMENSION, loadedIndex.getDimension());
                assertEquals(NUM_VECTORS, loadedIndex.getCount());

                SearchResult loadedResult = loadedIndex.searchSingle(query, K);
                assertArrayEquals(originalResult.getLabels(), loadedResult.getLabels());
            }
        }

        // Test byte array serialization
        try (Index index = IndexFactory.createFlat(DIMENSION)) {
            index.add(vectors);
            byte[] serialized = index.serialize();
            assertNotNull(serialized);
            assertTrue(serialized.length > 0);

            try (Index deserializedIndex = Index.deserialize(serialized)) {
                assertEquals(DIMENSION, deserializedIndex.getDimension());
                assertEquals(NUM_VECTORS, deserializedIndex.getCount());

                SearchResult deserializedResult = deserializedIndex.searchSingle(query, K);
                assertArrayEquals(originalResult.getLabels(), deserializedResult.getLabels());
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
        try (Index index = IndexFactory.createHNSW(DIMENSION, 32, MetricType.L2)) {
            assertTrue(index.isTrained()); // HNSW doesn't need training

            float[] vectors = generateRandomVectors(NUM_VECTORS, DIMENSION);
            index.add(vectors);

            // Get and set efSearch
            int efSearch = IndexHNSW.getEfSearch(index);
            assertTrue(efSearch > 0);

            IndexHNSW.setEfSearch(index, 64);
            assertEquals(64, IndexHNSW.getEfSearch(index));

            // Search
            float[] query = generateRandomVectors(1, DIMENSION);
            SearchResult result = index.searchSingle(query, K);
            assertEquals(K, result.getLabels().length);
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

        // Test vector dimension mismatch
        try (Index index = IndexFactory.createFlat(DIMENSION)) {
            float[] wrongDimVectors = new float[10]; // Wrong size
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        index.addSingle(wrongDimVectors);
                    });
        }

        // Test closed index
        Index closedIndex = IndexFactory.createFlat(DIMENSION);
        closedIndex.close();
        assertThrows(
                IllegalStateException.class,
                () -> {
                    closedIndex.getCount();
                });
    }

    @Test
    void testSearchResultAccessors() {
        try (Index index = IndexFactory.createFlat(DIMENSION)) {
            float[] vectors = generateRandomVectors(100, DIMENSION);
            index.add(vectors);

            float[] queries = generateRandomVectors(3, DIMENSION);
            SearchResult result = index.search(queries, 5);

            // Test individual accessors
            for (int q = 0; q < 3; q++) {
                for (int n = 0; n < 5; n++) {
                    long label = result.getLabel(q, n);
                    float distance = result.getDistance(q, n);
                    assertTrue(label >= 0 && label < 100);
                    assertTrue(distance >= 0);
                }
            }

            // Test out of bounds
            assertThrows(
                    IndexOutOfBoundsException.class,
                    () -> {
                        result.getLabel(10, 0);
                    });
            assertThrows(
                    IndexOutOfBoundsException.class,
                    () -> {
                        result.getLabel(0, 10);
                    });
        }
    }

    private float[] generateRandomVectors(int n, int d) {
        Random random = new Random(42);
        float[] vectors = new float[n * d];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = random.nextFloat();
        }
        return vectors;
    }
}
