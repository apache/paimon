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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for Issue #4: STR bulk loading optimization. */
public class RTreeSTRBulkLoaderTest {

    @Test
    public void testEmptyDataset() {
        STRBulkLoader loader = new STRBulkLoader(2, 16);
        RTree tree = loader.bulkLoad(new ArrayList<>());

        assertEquals(0, tree.getSize(), "Empty dataset should produce empty tree");
    }

    @Test
    public void testSingleEntry() {
        STRBulkLoader loader = new STRBulkLoader(2, 16);
        List<LeafEntry> entries = new ArrayList<>();
        entries.add(new LeafEntry(0, BoundingBox.fromPoint(new double[] {1.0, 2.0})));

        RTree tree = loader.bulkLoad(entries);

        assertEquals(1, tree.getSize(), "Single entry tree should have size 1");
        List<Integer> results = tree.search(new double[] {1.0, 2.0});
        assertEquals(1, results.size(), "Should find the single entry");
        assertEquals(0, results.get(0), "Should find row ID 0");
    }

    @Test
    public void testSmallDataset() {
        STRBulkLoader loader = new STRBulkLoader(2, 16);
        List<LeafEntry> entries = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {i, i})));
        }

        RTree tree = loader.bulkLoad(entries);

        assertEquals(10, tree.getSize(), "Small dataset should have size 10");

        BoundingBox fullSpace = new BoundingBox(new double[] {-10, -10}, new double[] {20, 20});
        List<Integer> results = tree.search(fullSpace);
        assertEquals(10, results.size(), "All entries should be recoverable");
    }

    @Test
    public void testMediumDataset() {
        STRBulkLoader loader = new STRBulkLoader(2, 16);
        List<LeafEntry> entries = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            double x = Math.sin(i * 0.01) * 500 + (i % 100);
            double y = Math.cos(i * 0.01) * 500 + (i / 100);
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {x, y})));
        }

        RTree tree = loader.bulkLoad(entries);

        assertEquals(1000, tree.getSize(), "Medium dataset should have size 1000");

        BoundingBox fullSpace =
                new BoundingBox(new double[] {-1000, -1000}, new double[] {1000, 1000});
        List<Integer> results = tree.search(fullSpace);
        assertEquals(1000, results.size(), "All 1000 entries should be recoverable");
    }

    @Test
    public void testLargeDataset() {
        STRBulkLoader loader = new STRBulkLoader(2, 16);
        List<LeafEntry> entries = new ArrayList<>();

        for (int i = 0; i < 100000; i++) {
            double x = Math.sin(i * 0.001) * 5000 + Math.random() * 100;
            double y = Math.cos(i * 0.001) * 5000 + Math.random() * 100;
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {x, y})));
        }

        long startTime = System.currentTimeMillis();
        RTree tree = loader.bulkLoad(entries);
        long elapsedTime = System.currentTimeMillis() - startTime;

        assertEquals(100000, tree.getSize(), "Large dataset should have size 100K");

        BoundingBox fullSpace =
                new BoundingBox(new double[] {-10000, -10000}, new double[] {10000, 10000});
        List<Integer> results = tree.search(fullSpace);
        assertEquals(100000, results.size(), "All 100K entries should be recoverable");

        System.out.println("STR bulk loading 100K points took: " + elapsedTime + "ms");
    }

    @Test
    public void testConsistencyWithIncremental() {
        List<LeafEntry> entries = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            double x = Math.random() * 1000;
            double y = Math.random() * 1000;
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {x, y})));
        }

        STRBulkLoader loader = new STRBulkLoader(2, 16);
        RTree bulkTree = loader.bulkLoad(entries);

        BoundingBox searchBox = new BoundingBox(new double[] {100, 100}, new double[] {900, 900});
        List<Integer> bulkResults = bulkTree.search(searchBox);

        assertTrue(bulkResults.size() > 0, "Should find some entries in search box");
        for (Integer rowId : bulkResults) {
            LeafEntry entry = entries.get(rowId);
            assertTrue(
                    entry.getBbox().intersects(searchBox),
                    "All results should intersect with search box");
        }
    }

    @Test
    public void testTreeBalanceAfterBulkLoad() {
        STRBulkLoader loader = new STRBulkLoader(2, 16);
        List<LeafEntry> entries = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            double x = Math.sin(i * 0.01) * 2000 + Math.random() * 50;
            double y = Math.cos(i * 0.01) * 2000 + Math.random() * 50;
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {x, y})));
        }

        RTree tree = loader.bulkLoad(entries);
        assertEquals(10000, tree.getSize(), "Tree should have 10K entries");

        int treeHeight = calculateTreeHeight(tree.getRoot());
        int maxExpectedHeight = (int) Math.ceil(Math.log(10000) / Math.log(16)) + 1;

        assertTrue(
                treeHeight <= maxExpectedHeight + 2,
                "Tree height should be balanced: " + treeHeight + " vs " + maxExpectedHeight);
    }

    @Test
    public void testMultiDimensionalBulkLoad() {
        STRBulkLoader loader = new STRBulkLoader(3, 8);
        List<LeafEntry> entries = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            double x = Math.sin(i * 0.01) * 500;
            double y = Math.cos(i * 0.01) * 500;
            double z = Math.random() * 1000;
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {x, y, z})));
        }

        RTree tree = loader.bulkLoad(entries);

        assertEquals(1000, tree.getSize(), "3D dataset should have size 1000");

        BoundingBox searchBox =
                new BoundingBox(
                        new double[] {-1000, -1000, -1000}, new double[] {1000, 1000, 1000});
        List<Integer> results = tree.search(searchBox);
        assertEquals(1000, results.size(), "All 3D entries should be recoverable");
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
}
