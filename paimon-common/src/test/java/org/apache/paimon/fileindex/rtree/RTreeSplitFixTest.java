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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test to verify that node splitting correctly maintains tree integrity. */
public class RTreeSplitFixTest {

    @Test
    public void testSplitWithManyInsertions() {
        RTree rtree = new RTree(2, 4);

        for (int i = 0; i < 100; i++) {
            rtree.insert(new double[] {i, i}, i);
        }

        for (int i = 0; i < 100; i++) {
            BoundingBox bbox = BoundingBox.fromPoint(new double[] {i, i});
            List<Integer> results = rtree.search(bbox);
            assertTrue(results.contains(i), "Data " + i + " was lost after splits!");
        }
    }

    @Test
    public void testSplitWithLargeDataset() {
        RTree rtree = new RTree(2, 32);

        for (int i = 0; i < 1000; i++) {
            rtree.insert(new double[] {i % 100, i / 100}, i);
        }

        int foundCount = 0;
        for (int i = 0; i < 1000; i++) {
            BoundingBox bbox = BoundingBox.fromPoint(new double[] {i % 100, i / 100});
            List<Integer> results = rtree.search(bbox);
            if (results.contains(i)) {
                foundCount++;
            }
        }

        assertEquals(1000, foundCount, "All 1000 records should be found after splits");
    }

    @Test
    public void testMultipleLevelSplits() {
        RTree rtree = new RTree(2, 8);

        for (int i = 0; i < 500; i++) {
            rtree.insert(new double[] {i * 0.1, i * 0.2}, i);
        }

        int totalFound = 0;
        for (int i = 0; i < 500; i++) {
            BoundingBox bbox = BoundingBox.fromPoint(new double[] {i * 0.1, i * 0.2});
            List<Integer> results = rtree.search(bbox);
            if (!results.isEmpty()) {
                totalFound++;
            }
        }

        assertTrue(totalFound > 0, "Should find records even with multiple level splits");
        assertEquals(500, totalFound, "All records should be searchable");
    }

    @Test
    public void testParentPointerAfterSplit() {
        RTree rtree = new RTree(2, 4);

        for (int i = 0; i < 20; i++) {
            rtree.insert(new double[] {i, i}, i);
        }

        RTreeNode root = rtree.getRoot();
        for (RTreeNode child : root.getChildren()) {
            assertEquals(root, child.getParent(), "Child parent pointer should point to root");
        }
    }

    @Test
    public void testRangeQueryAfterSplits() {
        RTree rtree = new RTree(2, 16);

        for (int i = 0; i < 200; i++) {
            rtree.insert(new double[] {i % 50, i / 50}, i);
        }

        BoundingBox queryBox = new BoundingBox(new double[] {10, 1}, new double[] {30, 3});
        List<Integer> results = rtree.search(queryBox);

        assertTrue(results.size() > 0, "Range query should return results after splits");
    }

    @Test
    public void testTreeHeightReasonable() {
        RTree rtree = new RTree(2, 32);

        for (int i = 0; i < 10000; i++) {
            rtree.insert(new double[] {i % 100, i / 100}, i);
        }

        int height = getTreeHeight(rtree.getRoot());
        int expectedMaxHeight = (int) (Math.log(10000) / Math.log(32)) + 2;

        assertTrue(
                height <= expectedMaxHeight,
                "Tree height " + height + " should be reasonable (max: " + expectedMaxHeight + ")");
    }

    @Test
    public void testAllDataRecoverable() {
        RTree rtree = new RTree(2, 24);

        for (int i = 0; i < 5000; i++) {
            rtree.insert(new double[] {Math.random() * 1000, Math.random() * 1000}, i);
        }

        BoundingBox wholeSpace = new BoundingBox(new double[] {0, 0}, new double[] {10000, 10000});
        List<Integer> allResults = rtree.search(wholeSpace);

        assertEquals(
                5000,
                allResults.size(),
                "All 5000 inserted records should be recoverable via full range query");
    }

    private int getTreeHeight(RTreeNode node) {
        if (node.isLeaf()) {
            return 1;
        }
        int maxChildHeight = 0;
        for (RTreeNode child : node.getChildren()) {
            maxChildHeight = Math.max(maxChildHeight, getTreeHeight(child));
        }
        return 1 + maxChildHeight;
    }
}
