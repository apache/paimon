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

package org.apache.paimon.fileindex.rtree;

import java.util.List;
import java.util.Random;

/** Benchmark for R-Tree performance. */
public class RTreeBenchmark {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int BENCHMARK_ITERATIONS = 10;

    public static void main(String[] args) {
        System.out.println("========== R-Tree Spatial Index Benchmark ==========\n");

        benchmarkInsertion();
        benchmarkSearch();
        benchmarkSequentialAccess();
        benchmarkHighDimensional();
        benchmarkRandomDistribution();

        System.out.println("\n========== Benchmark Complete ==========");
    }

    private static void benchmarkInsertion() {
        System.out.println("### Insertion Performance ###");
        System.out.println("Testing R-Tree construction with varying dataset sizes...\n");

        int[] sizes = {1000, 10000, 50000, 100000};
        int[] maxEntries = {8, 16, 32, 64};

        for (int size : sizes) {
            System.out.println("Dataset size: " + size + " points");
            for (int maxEntry : maxEntries) {
                benchmarkInsertionForSize(size, maxEntry);
            }
            System.out.println();
        }
    }

    private static void benchmarkInsertionForSize(int size, int maxEntry) {
        Random random = new Random(42);
        double[] points = generateRandomPoints(size, random);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            RTree rtree = new RTree(2, maxEntry);
            for (int j = 0; j < size; j++) {
                rtree.insert(new double[] {points[j * 2], points[j * 2 + 1]}, j);
            }
        }

        // Benchmark
        long totalTime = 0;
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            RTree rtree = new RTree(2, maxEntry);
            long startTime = System.nanoTime();
            for (int j = 0; j < size; j++) {
                rtree.insert(new double[] {points[j * 2], points[j * 2 + 1]}, j);
            }
            long endTime = System.nanoTime();
            totalTime += (endTime - startTime);
        }

        long averageTime = totalTime / BENCHMARK_ITERATIONS;
        double timePerPoint = (double) averageTime / size / 1000; // microseconds
        System.out.printf(
                "  max-entries=%2d: %10d ns (%.2f µs per point, %.2f K ops/s)\n",
                maxEntry, averageTime, timePerPoint, 1_000_000_000.0 / averageTime * size);
    }

    private static void benchmarkSearch() {
        System.out.println("### Search Performance ###");
        System.out.println("Testing spatial queries with different query patterns...\n");

        int datasetSize = 100000;
        Random random = new Random(42);
        double[] points = generateRandomPoints(datasetSize, random);

        RTree rtree = new RTree(2, 32);
        for (int i = 0; i < datasetSize; i++) {
            rtree.insert(new double[] {points[i * 2], points[i * 2 + 1]}, i);
        }

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            searchQueries(rtree, datasetSize, random, 10);
        }

        // Point queries
        long pointQueryTime = 0;
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            long startTime = System.nanoTime();
            searchQueries(rtree, datasetSize, random, 1000);
            long endTime = System.nanoTime();
            pointQueryTime += (endTime - startTime);
        }
        double pointQueryPerQuery = (double) pointQueryTime / BENCHMARK_ITERATIONS / 1000;
        System.out.printf("Point queries (1000 queries): %.2f µs per query\n", pointQueryPerQuery);

        // Warmup for range queries
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            rangeSearchQueries(rtree, datasetSize, random, 10);
        }

        // Range queries
        long rangeQueryTime = 0;
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            long startTime = System.nanoTime();
            rangeSearchQueries(rtree, datasetSize, random, 100);
            long endTime = System.nanoTime();
            rangeQueryTime += (endTime - startTime);
        }
        double rangeQueryPerQuery = (double) rangeQueryTime / BENCHMARK_ITERATIONS / 100;
        System.out.printf("Range queries (100 queries): %.2f µs per query\n\n", rangeQueryPerQuery);

        // Linear scan baseline
        long linearScanTime = 0;
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            long startTime = System.nanoTime();
            linearScan(points, 100);
            long endTime = System.nanoTime();
            linearScanTime += (endTime - startTime);
        }
        double linearScanPerQuery = (double) linearScanTime / BENCHMARK_ITERATIONS / 100;
        System.out.printf(
                "Linear scan baseline (100 scans): %.2f µs per scan\n", linearScanPerQuery);
        System.out.printf("Speedup: %.2fx\n\n", linearScanPerQuery / rangeQueryPerQuery);
    }

    private static void benchmarkSequentialAccess() {
        System.out.println("### Sequential Data Access Pattern ###");
        System.out.println("Testing performance with sequential spatial data...\n");

        int gridSize = 1000;
        RTree rtree = new RTree(2, 32);

        // Generate sequential/clustered data
        int idx = 0;
        for (int i = 0; i < gridSize; i++) {
            for (int j = 0; j < gridSize; j++) {
                rtree.insert(new double[] {i, j}, idx++);
            }
        }

        // Query clustered area
        long clusterQueryTime = 0;
        for (int iter = 0; iter < BENCHMARK_ITERATIONS; iter++) {
            long startTime = System.nanoTime();
            BoundingBox bbox = new BoundingBox(new double[] {100, 100}, new double[] {200, 200});
            List<Integer> results = rtree.search(bbox);
            long endTime = System.nanoTime();
            clusterQueryTime += (endTime - startTime);
            System.out.printf(
                    "  Iteration %d: %d results in %.2f µs\n",
                    iter + 1, results.size(), (endTime - startTime) / 1000.0);
        }

        double avgClusterQueryTime = (double) clusterQueryTime / BENCHMARK_ITERATIONS;
        System.out.printf("Average cluster query: %.2f µs\n\n", avgClusterQueryTime / 1000);
    }

    private static void benchmarkHighDimensional() {
        System.out.println("### High Dimensional Data ###");
        System.out.println("Testing performance with 3D and higher dimensions...\n");

        int datasetSize = 10000;
        Random random = new Random(42);

        int[] dimensions = {2, 3, 4, 5};
        for (int dim : dimensions) {
            double[] points = generateRandomPoints(datasetSize * dim / 2, random);

            RTree rtree = new RTree(dim, 16);

            long insertTime = 0;
            for (int i = 0; i < datasetSize; i++) {
                long startTime = System.nanoTime();
                double[] point = new double[dim];
                for (int d = 0; d < dim; d++) {
                    point[d] = random.nextDouble() * 100;
                }
                rtree.insert(point, i);
                long endTime = System.nanoTime();
                insertTime += (endTime - startTime);
            }

            double avgInsertTime = (double) insertTime / datasetSize;
            System.out.printf("%dD: %.2f µs per insert\n", dim, avgInsertTime / 1000);
        }
        System.out.println();
    }

    private static void benchmarkRandomDistribution() {
        System.out.println("### Random vs Clustered Distribution ###");
        System.out.println("Comparing performance impact of data distribution...\n");

        int datasetSize = 50000;
        Random random = new Random(42);

        // Random distribution
        RTree randomTree = new RTree(2, 32);
        long randomInsertTime = 0;
        for (int i = 0; i < datasetSize; i++) {
            double x = random.nextDouble() * 10000;
            double y = random.nextDouble() * 10000;
            long startTime = System.nanoTime();
            randomTree.insert(new double[] {x, y}, i);
            long endTime = System.nanoTime();
            randomInsertTime += (endTime - startTime);
        }

        // Clustered distribution
        RTree clusteredTree = new RTree(2, 32);
        long clusteredInsertTime = 0;
        random = new Random(42);
        for (int i = 0; i < datasetSize; i++) {
            // Create 10 clusters
            int cluster = i % 10;
            double clusterCenterX = (cluster % 3) * 3000 + 500;
            double clusterCenterY = (cluster / 3) * 3000 + 500;
            double x = clusterCenterX + random.nextGaussian() * 300;
            double y = clusterCenterY + random.nextGaussian() * 300;
            long startTime = System.nanoTime();
            clusteredTree.insert(new double[] {x, y}, i);
            long endTime = System.nanoTime();
            clusteredInsertTime += (endTime - startTime);
        }

        System.out.printf(
                "Random distribution insert: %.2f µs per point\n",
                randomInsertTime / 1000.0 / datasetSize);
        System.out.printf(
                "Clustered distribution insert: %.2f µs per point\n",
                clusteredInsertTime / 1000.0 / datasetSize);
        System.out.printf("Ratio: %.2fx\n\n", (double) randomInsertTime / clusteredInsertTime);

        // Query performance comparison
        System.out.println("Query performance comparison:");

        long randomQueryTime = 0;
        for (int i = 0; i < 100; i++) {
            BoundingBox bbox =
                    new BoundingBox(
                            new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000},
                            new double[] {
                                random.nextDouble() * 10000, random.nextDouble() * 10000
                            });
            long startTime = System.nanoTime();
            randomTree.search(bbox);
            long endTime = System.nanoTime();
            randomQueryTime += (endTime - startTime);
        }

        long clusteredQueryTime = 0;
        random = new Random(42);
        for (int i = 0; i < 100; i++) {
            BoundingBox bbox =
                    new BoundingBox(
                            new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000},
                            new double[] {
                                random.nextDouble() * 10000, random.nextDouble() * 10000
                            });
            long startTime = System.nanoTime();
            clusteredTree.search(bbox);
            long endTime = System.nanoTime();
            clusteredQueryTime += (endTime - startTime);
        }

        System.out.printf(
                "Random distribution query: %.2f µs per query\n", randomQueryTime / 100.0 / 1000);
        System.out.printf(
                "Clustered distribution query: %.2f µs per query\n",
                clusteredQueryTime / 100.0 / 1000);
    }

    private static double[] generateRandomPoints(int count, Random random) {
        double[] points = new double[count * 2];
        for (int i = 0; i < count * 2; i++) {
            points[i] = random.nextDouble() * 10000;
        }
        return points;
    }

    private static void searchQueries(RTree rtree, int datasetSize, Random random, int queryCount) {
        for (int i = 0; i < queryCount; i++) {
            double x = random.nextDouble() * 10000;
            double y = random.nextDouble() * 10000;
            rtree.search(new double[] {x, y});
        }
    }

    private static void rangeSearchQueries(
            RTree rtree, int datasetSize, Random random, int queryCount) {
        for (int i = 0; i < queryCount; i++) {
            double x1 = random.nextDouble() * 9000;
            double y1 = random.nextDouble() * 9000;
            double x2 = x1 + random.nextDouble() * 1000;
            double y2 = y1 + random.nextDouble() * 1000;
            BoundingBox bbox = new BoundingBox(new double[] {x1, y1}, new double[] {x2, y2});
            rtree.search(bbox);
        }
    }

    private static void linearScan(double[] points, int queryCount) {
        Random random = new Random(42);
        for (int q = 0; q < queryCount; q++) {
            double qx = random.nextDouble() * 10000;
            double qy = random.nextDouble() * 10000;
            double range = 500;
            int count = 0;
            for (int i = 0; i < points.length; i += 2) {
                double dx = points[i] - qx;
                double dy = points[i + 1] - qy;
                if (dx * dx + dy * dy <= range * range) {
                    count++;
                }
            }
        }
    }
}
