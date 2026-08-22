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

/**
 * Comparison benchmark: R-Tree vs Linear Scan.
 *
 * <p>This benchmark demonstrates the performance improvements of R-Tree indexing over simple linear
 * scans, showing significant speedup for spatial queries.
 */
public class RTreeVsLinearScanBenchmark {

    static class DataGenerator {
        private final double[] points;
        private final int count;

        DataGenerator(int count, Random random, String distribution) {
            this.count = count;
            this.points = new double[count * 2];

            if ("random".equals(distribution)) {
                for (int i = 0; i < count * 2; i++) {
                    points[i] = random.nextDouble() * 10000;
                }
            } else if ("clustered".equals(distribution)) {
                // 10 clusters
                for (int i = 0; i < count; i++) {
                    int cluster = i % 10;
                    double clusterX = (cluster % 3) * 3000 + 500;
                    double clusterY = (cluster / 3) * 3000 + 500;
                    points[i * 2] = clusterX + random.nextGaussian() * 400;
                    points[i * 2 + 1] = clusterY + random.nextGaussian() * 400;
                }
            } else if ("grid".equals(distribution)) {
                int side = (int) Math.sqrt(count);
                int idx = 0;
                for (int i = 0; i < side && idx < count; i++) {
                    for (int j = 0; j < side && idx < count; j++) {
                        points[idx * 2] = i * (10000 / side);
                        points[idx * 2 + 1] = j * (10000 / side);
                        idx++;
                    }
                }
            }
        }

        double[] getPoints() {
            return points;
        }
    }

    public static void main(String[] args) {
        System.out.println("========== R-Tree vs Linear Scan Benchmark ==========\n");

        benchmarkComparison();
        benchmarkByDatasetSize();
        benchmarkByDistribution();
        benchmarkByQueryPattern();

        System.out.println("\n========== Benchmark Complete ==========");
    }

    private static void benchmarkComparison() {
        System.out.println("### Overall Comparison (100K points) ###\n");

        Random random = new Random(42);
        DataGenerator generator = new DataGenerator(100000, random, "random");
        double[] points = generator.getPoints();

        // Build R-Tree
        RTree rtree = new RTree(2, 32);
        long rtreeInsertTime = 0;
        for (int i = 0; i < 100000; i++) {
            long startTime = System.nanoTime();
            rtree.insert(new double[] {points[i * 2], points[i * 2 + 1]}, i);
            long endTime = System.nanoTime();
            rtreeInsertTime += (endTime - startTime);
        }

        System.out.println("Construction Time:");
        System.out.printf("  R-Tree indexing: %d ms\n", rtreeInsertTime / 1_000_000);
        System.out.printf("  (%.2f µs per point)\n\n", rtreeInsertTime / 1000.0 / 100000);

        // Query performance comparison
        System.out.println("Query Performance (10,000 queries):");

        // R-Tree queries
        long rtreeQueryTime = 0;
        int resultCount = 0;
        random = new Random(42);
        for (int i = 0; i < 10000; i++) {
            BoundingBox bbox =
                    new BoundingBox(
                            new double[] {random.nextDouble() * 9500, random.nextDouble() * 9500},
                            new double[] {
                                random.nextDouble() * 10000, random.nextDouble() * 10000
                            });
            long startTime = System.nanoTime();
            List<Integer> results = rtree.search(bbox);
            long endTime = System.nanoTime();
            rtreeQueryTime += (endTime - startTime);
            resultCount += results.size();
        }

        // Linear scan queries
        long linearScanTime = 0;
        int linearResultCount = 0;
        random = new Random(42);
        for (int i = 0; i < 10000; i++) {
            double x1 = random.nextDouble() * 9500;
            double y1 = random.nextDouble() * 9500;
            double x2 = random.nextDouble() * 10000;
            double y2 = random.nextDouble() * 10000;
            double minX = Math.min(x1, x2);
            double maxX = Math.max(x1, x2);
            double minY = Math.min(y1, y2);
            double maxY = Math.max(y1, y2);

            long startTime = System.nanoTime();
            int count = 0;
            for (int j = 0; j < 100000; j++) {
                double px = points[j * 2];
                double py = points[j * 2 + 1];
                if (px >= minX && px <= maxX && py >= minY && py <= maxY) {
                    count++;
                }
            }
            long endTime = System.nanoTime();
            linearScanTime += (endTime - startTime);
            linearResultCount += count;
        }

        double rtreeAvgTime = rtreeQueryTime / 10000.0 / 1000;
        double linearAvgTime = linearScanTime / 10000.0 / 1000;
        double speedup = linearScanTime / (double) rtreeQueryTime;

        System.out.printf("  R-Tree:       %.2f µs per query\n", rtreeAvgTime);
        System.out.printf("  Linear Scan:  %.2f µs per query\n", linearAvgTime);
        System.out.printf("  Speedup:      %.2f×\n", speedup);
        System.out.printf("  Avg results per query: %d\n\n", resultCount / 10000);
    }

    private static void benchmarkByDatasetSize() {
        System.out.println("### Performance by Dataset Size ###\n");

        int[] sizes = {1000, 10000, 100000, 1000000};
        Random random = new Random(42);

        System.out.println("Dataset Size | R-Tree (µs) | Linear (µs) | Speedup");
        System.out.println("-------------|-------------|-------------|--------");

        for (int size : sizes) {
            DataGenerator generator = new DataGenerator(size, random, "random");
            double[] points = generator.getPoints();

            // Build R-Tree
            RTree rtree = new RTree(2, 32);
            for (int i = 0; i < size; i++) {
                rtree.insert(new double[] {points[i * 2], points[i * 2 + 1]}, i);
            }

            // Run queries
            long rtreeTime = 0;
            long linearTime = 0;
            random = new Random(42);

            for (int q = 0; q < 100; q++) {
                BoundingBox bbox =
                        new BoundingBox(
                                new double[] {
                                    random.nextDouble() * 9500, random.nextDouble() * 9500
                                },
                                new double[] {
                                    random.nextDouble() * 10000, random.nextDouble() * 10000
                                });

                // R-Tree query
                long start = System.nanoTime();
                rtree.search(bbox);
                long end = System.nanoTime();
                rtreeTime += (end - start);

                // Linear scan
                double x1 = bbox.getMin()[0];
                double y1 = bbox.getMin()[1];
                double x2 = bbox.getMax()[0];
                double y2 = bbox.getMax()[1];
                start = System.nanoTime();
                for (int i = 0; i < size; i++) {
                    double px = points[i * 2];
                    double py = points[i * 2 + 1];
                    if (px >= x1 && px <= x2 && py >= y1 && py <= y2) {
                        // Found match
                    }
                }
                end = System.nanoTime();
                linearTime += (end - start);
            }

            double rtreeAvg = rtreeTime / 100.0 / 1000;
            double linearAvg = linearTime / 100.0 / 1000;
            double speedup = linearTime / (double) rtreeTime;

            System.out.printf(
                    " %8d    | %10.2f | %10.2f | %.2f×\n", size, rtreeAvg, linearAvg, speedup);
        }
        System.out.println();
    }

    private static void benchmarkByDistribution() {
        System.out.println("### Performance by Data Distribution ###\n");

        String[] distributions = {"random", "clustered", "grid"};
        Random random = new Random(42);
        int datasetSize = 100000;

        System.out.println("Distribution | R-Tree (µs) | Linear (µs) | Speedup");
        System.out.println("-------------|-------------|-------------|--------");

        for (String distribution : distributions) {
            DataGenerator generator = new DataGenerator(datasetSize, random, distribution);
            double[] points = generator.getPoints();

            // Build R-Tree
            RTree rtree = new RTree(2, 32);
            for (int i = 0; i < datasetSize; i++) {
                rtree.insert(new double[] {points[i * 2], points[i * 2 + 1]}, i);
            }

            // Run queries
            long rtreeTime = 0;
            long linearTime = 0;
            random = new Random(42);

            for (int q = 0; q < 1000; q++) {
                BoundingBox bbox =
                        new BoundingBox(
                                new double[] {
                                    random.nextDouble() * 9500, random.nextDouble() * 9500
                                },
                                new double[] {
                                    random.nextDouble() * 10000, random.nextDouble() * 10000
                                });

                // R-Tree query
                long start = System.nanoTime();
                rtree.search(bbox);
                long end = System.nanoTime();
                rtreeTime += (end - start);

                // Linear scan
                double x1 = bbox.getMin()[0];
                double y1 = bbox.getMin()[1];
                double x2 = bbox.getMax()[0];
                double y2 = bbox.getMax()[1];
                start = System.nanoTime();
                for (int i = 0; i < datasetSize; i++) {
                    double px = points[i * 2];
                    double py = points[i * 2 + 1];
                    if (px >= x1 && px <= x2 && py >= y1 && py <= y2) {
                        // Found match
                    }
                }
                end = System.nanoTime();
                linearTime += (end - start);
            }

            double rtreeAvg = rtreeTime / 1000.0 / 1000;
            double linearAvg = linearTime / 1000.0 / 1000;
            double speedup = linearTime / (double) rtreeTime;

            System.out.printf(
                    " %10s   | %10.2f | %10.2f | %.2f×\n",
                    distribution, rtreeAvg, linearAvg, speedup);
        }
        System.out.println();
    }

    private static void benchmarkByQueryPattern() {
        System.out.println("### Performance by Query Pattern ###\n");

        Random random = new Random(42);
        DataGenerator generator = new DataGenerator(100000, random, "random");
        double[] points = generator.getPoints();

        RTree rtree = new RTree(2, 32);
        for (int i = 0; i < 100000; i++) {
            rtree.insert(new double[] {points[i * 2], points[i * 2 + 1]}, i);
        }

        System.out.println("Query Type      | R-Tree (µs) | Linear (µs) | Speedup | Selectivity");
        System.out.println("-----------------|-------------|-------------|--------|-------------");

        // Small region (high selectivity)
        benchmarkQueryPattern(rtree, points, 100000, "Small region", 500, random);

        // Medium region (medium selectivity)
        benchmarkQueryPattern(rtree, points, 100000, "Medium region", 1500, random);

        // Large region (low selectivity)
        benchmarkQueryPattern(rtree, points, 100000, "Large region", 5000, random);

        System.out.println();
    }

    private static void benchmarkQueryPattern(
            RTree rtree,
            double[] points,
            int datasetSize,
            String label,
            double querySize,
            Random random) {
        long rtreeTime = 0;
        long linearTime = 0;
        int totalResults = 0;

        for (int q = 0; q < 1000; q++) {
            double x1 = random.nextDouble() * (10000 - querySize);
            double y1 = random.nextDouble() * (10000 - querySize);
            BoundingBox bbox =
                    new BoundingBox(
                            new double[] {x1, y1}, new double[] {x1 + querySize, y1 + querySize});

            // R-Tree query
            long start = System.nanoTime();
            List<Integer> results = rtree.search(bbox);
            long end = System.nanoTime();
            rtreeTime += (end - start);
            totalResults += results.size();

            // Linear scan
            double x2 = x1 + querySize;
            double y2 = y1 + querySize;
            start = System.nanoTime();
            int count = 0;
            for (int i = 0; i < datasetSize; i++) {
                double px = points[i * 2];
                double py = points[i * 2 + 1];
                if (px >= x1 && px <= x2 && py >= y1 && py <= y2) {
                    count++;
                }
            }
            end = System.nanoTime();
            linearTime += (end - start);
        }

        double rtreeAvg = rtreeTime / 1000.0 / 1000;
        double linearAvg = linearTime / 1000.0 / 1000;
        double speedup = linearTime / (double) rtreeTime;
        double selectivity = totalResults / 1000.0 / datasetSize * 100;

        System.out.printf(
                " %-14s | %10.2f | %10.2f | %.2f×  | %.2f%%\n",
                label, rtreeAvg, linearAvg, speedup, selectivity);
    }
}
