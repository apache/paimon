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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Performance benchmark comparing STR bulk loading vs incremental insertion. */
public class RTreePerformanceBenchmarkTest {

    @Test
    public void benchmarkIncrementalInsertionVsSTRBulkLoading() {
        System.out.println("\n=== RTree Performance Benchmark ===");
        System.out.println("Comparing incremental insertion vs STR bulk loading\n");

        int[] sizes = {1000, 10000, 100000};

        for (int size : sizes) {
            List<LeafEntry> entries = generateTestData(size);

            long incrementalTime = benchmarkIncremental(entries);
            long bulkLoadTime = benchmarkBulkLoad(entries);
            double speedup = (double) incrementalTime / bulkLoadTime;

            System.out.printf(
                    "Size: %,7d | Incremental: %5dms | Bulk Load: %5dms | Speedup: %.1fx%n",
                    size, incrementalTime, bulkLoadTime, speedup);
        }
    }

    private long benchmarkIncremental(List<LeafEntry> entries) {
        RTree tree = new RTree(2, 16);

        long startTime = System.currentTimeMillis();
        for (LeafEntry entry : entries) {
            double[] point = new double[2];
            point[0] = entry.getBbox().getMin()[0];
            point[1] = entry.getBbox().getMin()[1];
            tree.insert(point, entry.getRowId());
        }
        return System.currentTimeMillis() - startTime;
    }

    private long benchmarkBulkLoad(List<LeafEntry> entries) {
        STRBulkLoader loader = new STRBulkLoader(2, 16);

        long startTime = System.currentTimeMillis();
        RTree tree = loader.bulkLoad(entries);
        return System.currentTimeMillis() - startTime;
    }

    @Test
    public void benchmarkSearchPerformance() {
        System.out.println("\n=== Search Performance After Bulk Load ===\n");

        int size = 100000;
        List<LeafEntry> entries = generateTestData(size);

        STRBulkLoader loader = new STRBulkLoader(2, 16);
        RTree tree = loader.bulkLoad(entries);

        BoundingBox searchBox = new BoundingBox(new double[] {-500, -500}, new double[] {500, 500});

        long startTime = System.nanoTime();
        List<Integer> results = tree.search(searchBox);
        long elapsedNanos = System.nanoTime() - startTime;
        double elapsedMs = elapsedNanos / 1_000_000.0;

        System.out.printf(
                "100K dataset, query box [-500,500]x[-500,500]: %,d results in %.3fms%n",
                results.size(), elapsedMs);
    }

    @Test
    public void validateBulkLoadTreeQuality() {
        System.out.println("\n=== Tree Quality Metrics ===\n");

        int size = 50000;
        List<LeafEntry> entries = generateTestData(size);

        STRBulkLoader loader = new STRBulkLoader(2, 16);
        RTree bulkTree = loader.bulkLoad(entries);

        int treeHeight = calculateTreeHeight(bulkTree.getRoot());
        int nodeCount = countNodes(bulkTree.getRoot());
        double avgBboxOverlap = calculateAvgBboxOverlap(bulkTree.getRoot());

        System.out.printf("Dataset size: %,d%n", size);
        System.out.printf("Tree height: %d%n", treeHeight);
        System.out.printf("Node count: %,d%n", nodeCount);
        System.out.printf("Avg bbox overlap: %.1f%%%n", avgBboxOverlap * 100);

        assertEquals(size, bulkTree.getSize(), "Bulk loaded tree should contain all entries");
    }

    private List<LeafEntry> generateTestData(int size) {
        List<LeafEntry> entries = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            double x = Math.sin(i * 0.001) * 1000 + Math.random() * 100;
            double y = Math.cos(i * 0.001) * 1000 + Math.random() * 100;
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {x, y})));
        }
        return entries;
    }

    private int calculateTreeHeight(RTreeNode node) {
        if (node.isLeaf()) {
            return 1;
        }
        int maxChildHeight = 0;
        for (RTreeNode child : node.getChildren()) {
            maxChildHeight = Math.max(maxChildHeight, calculateTreeHeight(child));
        }
        return 1 + maxChildHeight;
    }

    private int countNodes(RTreeNode node) {
        if (node.isLeaf()) {
            return 1;
        }
        int count = 1;
        for (RTreeNode child : node.getChildren()) {
            count += countNodes(child);
        }
        return count;
    }

    private double calculateAvgBboxOverlap(RTreeNode node) {
        if (node.isLeaf() || node.getChildren().isEmpty()) {
            return 0.0;
        }

        double totalOverlap = 0.0;
        int pairCount = 0;

        List<RTreeNode> children = node.getChildren();
        for (int i = 0; i < children.size(); i++) {
            for (int j = i + 1; j < children.size(); j++) {
                BoundingBox bbox1 = children.get(i).getBoundingBox();
                BoundingBox bbox2 = children.get(j).getBoundingBox();
                double overlap = calculateOverlapArea(bbox1, bbox2);
                double totalArea = bbox1.getArea() + bbox2.getArea();
                if (totalArea > 0) {
                    totalOverlap += overlap / totalArea;
                    pairCount++;
                }
            }
        }

        double avgOverlapRatio = pairCount > 0 ? totalOverlap / pairCount : 0.0;

        for (RTreeNode child : children) {
            if (!child.isLeaf()) {
                avgOverlapRatio += calculateAvgBboxOverlap(child);
            }
        }

        return avgOverlapRatio / children.size();
    }

    private double calculateOverlapArea(BoundingBox b1, BoundingBox b2) {
        double[] min1 = b1.getMin();
        double[] max1 = b1.getMax();
        double[] min2 = b2.getMin();
        double[] max2 = b2.getMax();

        double overlapX = Math.max(0, Math.min(max1[0], max2[0]) - Math.max(min1[0], min2[0]));
        double overlapY = Math.max(0, Math.min(max1[1], max2[1]) - Math.max(min1[1], min2[1]));

        return overlapX * overlapY;
    }
}
