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

package org.apache.paimon.faiss.example;

import org.apache.paimon.faiss.Faiss;
import org.apache.paimon.faiss.Index;
import org.apache.paimon.faiss.IndexFactory;
import org.apache.paimon.faiss.IndexHNSW;
import org.apache.paimon.faiss.MetricType;
import org.apache.paimon.faiss.SearchResult;

import java.util.Random;

/**
 * Basic example demonstrating the usage of Paimon Faiss.
 *
 * <p>This example shows how to:
 * <ul>
 *   <li>Create different types of indexes</li>
 *   <li>Add vectors to an index</li>
 *   <li>Search for nearest neighbors</li>
 *   <li>Serialize and deserialize indexes</li>
 * </ul>
 */
public class BasicExample {

    private static final int DIMENSION = 128;
    private static final int NUM_VECTORS = 10000;
    private static final int K = 5;

    public static void main(String[] args) {
        System.out.println("Paimon Faiss JNI Basic Example");
        System.out.println("==========================");
        System.out.println("Faiss Version: " + Faiss.getVersion());
        System.out.println();

        // Generate random data
        Random random = new Random(42);
        float[] database = generateRandomVectors(random, NUM_VECTORS, DIMENSION);
        float[] query = generateRandomVectors(random, 1, DIMENSION);

        // Example 1: Flat Index (exact search)
        System.out.println("Example 1: Flat Index (Exact Search)");
        System.out.println("-------------------------------------");
        flatIndexExample(database, query);
        System.out.println();

        // Example 2: HNSW Index (approximate search)
        System.out.println("Example 2: HNSW Index (Approximate Search)");
        System.out.println("-------------------------------------------");
        hnswIndexExample(database, query);
        System.out.println();

        // Example 3: Index with custom IDs
        System.out.println("Example 3: Index with Custom IDs");
        System.out.println("---------------------------------");
        customIdsExample(database, query);
        System.out.println();

        // Example 4: Index serialization
        System.out.println("Example 4: Index Serialization");
        System.out.println("-------------------------------");
        serializationExample(database, query);
    }

    private static void flatIndexExample(float[] database, float[] query) {
        try (Index index = IndexFactory.createFlat(DIMENSION, MetricType.L2)) {
            System.out.println("Created flat index with dimension: " + index.getDimension());

            // Add vectors
            long startTime = System.currentTimeMillis();
            index.add(database);
            long addTime = System.currentTimeMillis() - startTime;
            System.out.println("Added " + index.getCount() + " vectors in " + addTime + " ms");

            // Search
            startTime = System.currentTimeMillis();
            SearchResult result = index.searchSingle(query, K);
            long searchTime = System.currentTimeMillis() - startTime;
            System.out.println("Search completed in " + searchTime + " ms");

            // Print results
            System.out.println("Top " + K + " results:");
            for (int i = 0; i < K; i++) {
                System.out.printf("  %d: id=%d, distance=%.4f%n",
                        i + 1, result.getLabel(0, i), result.getDistance(0, i));
            }
        }
    }

    private static void hnswIndexExample(float[] database, float[] query) {
        try (Index index = IndexFactory.createHNSW(DIMENSION, 32, MetricType.L2)) {
            System.out.println("Created HNSW index");

            // Add vectors
            long startTime = System.currentTimeMillis();
            index.add(database);
            long addTime = System.currentTimeMillis() - startTime;
            System.out.println("Added " + index.getCount() + " vectors in " + addTime + " ms");

            // Configure search parameters
            IndexHNSW.setEfSearch(index, 64);
            System.out.println("Set efSearch to 64");

            // Search
            startTime = System.currentTimeMillis();
            SearchResult result = index.searchSingle(query, K);
            long searchTime = System.currentTimeMillis() - startTime;
            System.out.println("Search completed in " + searchTime + " ms");

            // Print results
            System.out.println("Top " + K + " results:");
            for (int i = 0; i < K; i++) {
                System.out.printf("  %d: id=%d, distance=%.4f%n",
                        i + 1, result.getLabel(0, i), result.getDistance(0, i));
            }
        }
    }

    private static void customIdsExample(float[] database, float[] query) {
        try (Index index = IndexFactory.createFlatWithIds(DIMENSION, MetricType.L2)) {
            // Create custom IDs
            long[] ids = new long[NUM_VECTORS];
            for (int i = 0; i < NUM_VECTORS; i++) {
                ids[i] = 1000000L + i;  // Start from 1,000,000
            }

            // Add vectors with custom IDs
            index.addWithIds(database, ids);
            System.out.println("Added " + index.getCount() + " vectors with custom IDs");

            // Search
            SearchResult result = index.searchSingle(query, K);

            // Print results (should show custom IDs)
            System.out.println("Top " + K + " results (with custom IDs):");
            for (int i = 0; i < K; i++) {
                System.out.printf("  %d: id=%d, distance=%.4f%n",
                        i + 1, result.getLabel(0, i), result.getDistance(0, i));
            }
        }
    }

    private static void serializationExample(float[] database, float[] query) {
        byte[] serialized;

        // Create and serialize index
        try (Index index = IndexFactory.createFlat(DIMENSION, MetricType.L2)) {
            index.add(database);
            serialized = index.serialize();
            System.out.println("Serialized index to " + serialized.length + " bytes");
        }

        // Deserialize and search
        try (Index index = Index.deserialize(serialized)) {
            System.out.println("Deserialized index with " + index.getCount() + " vectors");

            SearchResult result = index.searchSingle(query, K);
            System.out.println("Search on deserialized index:");
            System.out.printf("  Top result: id=%d, distance=%.4f%n",
                    result.getLabel(0, 0), result.getDistance(0, 0));
        }
    }

    private static float[] generateRandomVectors(Random random, int n, int d) {
        float[] vectors = new float[n * d];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = random.nextFloat();
        }
        return vectors;
    }
}

