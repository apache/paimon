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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** JMH-style benchmark for R-Tree. To run with actual JMH, add org.openjdk.jmh dependencies. */
public class RTreeJMHBenchmark {

    // Benchmark state objects
    static class BenchmarkState {
        RTree rtree;
        List<double[]> queryPoints;
        List<BoundingBox> queryBoxes;
        Random random;

        void setup() {
            random = new Random(42);
        }

        void setupSmall() {
            setup();
            rtree = new RTree(2, 16);
            for (int i = 0; i < 1000; i++) {
                rtree.insert(
                        new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000}, i);
            }
            prepareQueries(1000);
        }

        void setupMedium() {
            setup();
            rtree = new RTree(2, 32);
            for (int i = 0; i < 50000; i++) {
                rtree.insert(
                        new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000}, i);
            }
            prepareQueries(50000);
        }

        void setupLarge() {
            setup();
            rtree = new RTree(2, 32);
            for (int i = 0; i < 1000000; i++) {
                rtree.insert(
                        new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000}, i);
            }
            prepareQueries(100000);
        }

        private void prepareQueries(int count) {
            queryPoints = new ArrayList<>();
            queryBoxes = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                queryPoints.add(
                        new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000});
                double x1 = random.nextDouble() * 9000;
                double y1 = random.nextDouble() * 9000;
                queryBoxes.add(
                        new BoundingBox(
                                new double[] {x1, y1}, new double[] {x1 + 1000, y1 + 1000}));
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("========== R-Tree JMH-Style Benchmark ==========\n");

        // Small dataset
        System.out.println("### Small Dataset (1,000 points) ###");
        benchmarkDatasetSize(
                "Small",
                new BenchmarkState() {
                    {
                        setupSmall();
                    }
                });

        // Medium dataset
        System.out.println("\n### Medium Dataset (50,000 points) ###");
        benchmarkDatasetSize(
                "Medium",
                new BenchmarkState() {
                    {
                        setupMedium();
                    }
                });

        // Large dataset
        System.out.println("\n### Large Dataset (1,000,000 points) ###");
        benchmarkDatasetSize(
                "Large",
                new BenchmarkState() {
                    {
                        setupLarge();
                    }
                });

        System.out.println("\n========== Benchmark Complete ==========");
    }

    private static void benchmarkDatasetSize(String label, BenchmarkState state) {
        // Point search
        long pointSearchTime = 0;
        for (int i = 0; i < state.queryPoints.size(); i++) {
            long startTime = System.nanoTime();
            state.rtree.search(state.queryPoints.get(i));
            long endTime = System.nanoTime();
            pointSearchTime += (endTime - startTime);
        }
        double avgPointSearchTime = pointSearchTime / 1000.0 / state.queryPoints.size();

        // Range search
        long rangeSearchTime = 0;
        for (int i = 0; i < state.queryBoxes.size(); i++) {
            long startTime = System.nanoTime();
            state.rtree.search(state.queryBoxes.get(i));
            long endTime = System.nanoTime();
            rangeSearchTime += (endTime - startTime);
        }
        double avgRangeSearchTime = rangeSearchTime / 1000.0 / state.queryBoxes.size();

        System.out.printf("Point search:  %.3f µs per operation\n", avgPointSearchTime);
        System.out.printf("Range search:  %.3f µs per operation\n", avgRangeSearchTime);
        System.out.printf(
                "Total throughput: %.0f K ops/sec\n",
                (state.queryPoints.size() + state.queryBoxes.size())
                        * 1000000.0
                        / (pointSearchTime + rangeSearchTime));
    }

    // Simpler performance test without JMH
    static class SimplePerformanceTest {
        void runTest() {
            System.out.println("========== Simple Performance Test ==========\n");

            // Test 1: Insertion performance
            System.out.println("Test 1: Insertion Performance");
            testInsertion();

            // Test 2: Query performance
            System.out.println("\nTest 2: Query Performance");
            testQuery();

            // Test 3: Memory efficiency
            System.out.println("\nTest 3: Memory Efficiency");
            testMemory();

            // Test 4: Scalability
            System.out.println("\nTest 4: Scalability");
            testScalability();
        }

        private void testInsertion() {
            int[] sizes = {10000, 100000, 500000};
            for (int size : sizes) {
                RTree rtree = new RTree(2, 32);
                Random random = new Random(42);

                long startTime = System.nanoTime();
                for (int i = 0; i < size; i++) {
                    rtree.insert(
                            new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000},
                            i);
                }
                long endTime = System.nanoTime();

                long totalTime = endTime - startTime;
                double timePerInsert = totalTime / 1000.0 / size;
                System.out.printf(
                        "  %7d points: %8d ms, %.2f µs per insert\n",
                        size, totalTime / 1_000_000, timePerInsert);
            }
        }

        private void testQuery() {
            RTree rtree = new RTree(2, 32);
            Random random = new Random(42);

            // Build tree with 100K points
            for (int i = 0; i < 100000; i++) {
                rtree.insert(
                        new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000}, i);
            }

            // Point queries
            long pointQueryTime = 0;
            int pointQueryCount = 10000;
            for (int i = 0; i < pointQueryCount; i++) {
                long startTime = System.nanoTime();
                rtree.search(
                        new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000});
                long endTime = System.nanoTime();
                pointQueryTime += (endTime - startTime);
            }

            // Range queries
            long rangeQueryTime = 0;
            int rangeQueryCount = 1000;
            for (int i = 0; i < rangeQueryCount; i++) {
                double x = random.nextDouble() * 9000;
                double y = random.nextDouble() * 9000;
                BoundingBox bbox =
                        new BoundingBox(new double[] {x, y}, new double[] {x + 1000, y + 1000});
                long startTime = System.nanoTime();
                rtree.search(bbox);
                long endTime = System.nanoTime();
                rangeQueryTime += (endTime - startTime);
            }

            System.out.printf(
                    "  Point queries (%d): %.2f µs per query\n",
                    pointQueryCount, pointQueryTime / 1000.0 / pointQueryCount);
            System.out.printf(
                    "  Range queries (%d): %.2f µs per query\n",
                    rangeQueryCount, rangeQueryTime / 1000.0 / rangeQueryCount);
        }

        private void testMemory() {
            System.out.println("  Analyzing memory usage patterns...");

            RTree rtree = new RTree(2, 32);
            Random random = new Random(42);

            long[] memorySamples = new long[5];
            int[] datasetSizes = {10000, 50000, 100000, 500000, 1000000};

            for (int idx = 0; idx < datasetSizes.length; idx++) {
                int size = datasetSizes[idx];
                Runtime runtime = Runtime.getRuntime();
                runtime.gc();
                long memBefore = runtime.totalMemory() - runtime.freeMemory();

                for (int i = 0; i < size; i++) {
                    rtree.insert(
                            new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000},
                            i);
                }

                runtime.gc();
                long memAfter = runtime.totalMemory() - runtime.freeMemory();
                memorySamples[idx] = (memAfter - memBefore) / 1024 / 1024; // MB

                System.out.printf("  %7d points: ~%3d MB\n", size, memorySamples[idx]);
            }
        }

        private void testScalability() {
            System.out.println("  Testing scalability with different max-entries...");

            int[] maxEntries = {8, 16, 32, 64, 128};
            int datasetSize = 100000;

            for (int maxEntry : maxEntries) {
                RTree rtree = new RTree(2, maxEntry);
                Random random = new Random(42);

                long insertTime = 0;
                for (int i = 0; i < datasetSize; i++) {
                    long startTime = System.nanoTime();
                    rtree.insert(
                            new double[] {random.nextDouble() * 10000, random.nextDouble() * 10000},
                            i);
                    long endTime = System.nanoTime();
                    insertTime += (endTime - startTime);
                }

                long queryTime = 0;
                for (int i = 0; i < 1000; i++) {
                    BoundingBox bbox =
                            new BoundingBox(
                                    new double[] {
                                        random.nextDouble() * 9000, random.nextDouble() * 9000
                                    },
                                    new double[] {
                                        random.nextDouble() * 10000, random.nextDouble() * 10000
                                    });
                    long startTime = System.nanoTime();
                    rtree.search(bbox);
                    long endTime = System.nanoTime();
                    queryTime += (endTime - startTime);
                }

                System.out.printf(
                        "  max-entries=%3d: insert=%.2f µs, query=%.2f µs\n",
                        maxEntry, insertTime / 1000.0 / datasetSize, queryTime / 1000.0 / 1000);
            }
        }
    }

    public static void runSimplePerformanceTest() {
        SimplePerformanceTest test = new SimplePerformanceTest();
        test.runTest();
    }
}
