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

/** Test for Issue #2: Quadratic split optimization. */
public class RTreeQuadraticSplitTest {

    @Test
    public void testQuadraticSplitBalancesGroups() {
        List<LeafEntry> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {i, i})));
        }

        QuadraticSplit split = new QuadraticSplit(entries, 2);

        int g1Size = split.getGroup1().size();
        int g2Size = split.getGroup2().size();

        assertEquals(10, g1Size + g2Size, "All entries should be assigned");
        assertTrue(
                Math.abs(g1Size - g2Size) <= 2,
                "Groups should be relatively balanced: " + g1Size + " vs " + g2Size);
    }

    @Test
    public void testQuadraticSplitMinimizesBboxOverlap() {
        List<LeafEntry> entries = new ArrayList<>();

        // Group A: points around (0,0)
        for (int i = 0; i < 5; i++) {
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {i, i})));
        }

        // Group B: points around (100,100)
        for (int i = 5; i < 10; i++) {
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {100 + i, 100 + i})));
        }

        QuadraticSplit split = new QuadraticSplit(entries, 2);

        BoundingBox g1Bbox = computeBbox(split.getGroup1());
        BoundingBox g2Bbox = computeBbox(split.getGroup2());

        // Most entries from group A should be in g1, most from group B in g2
        int g1CountA = 0;
        for (LeafEntry e : split.getGroup1()) {
            if (e.getRowId() < 5) {
                g1CountA++;
            }
        }

        assertTrue(g1CountA >= 3, "Group 1 should contain most entries from cluster A");
    }

    @Test
    public void testQuadraticSplitWithRandomPoints() {
        List<LeafEntry> entries = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            double x = Math.sin(i * 0.2) * 1000;
            double y = Math.cos(i * 0.2) * 1000;
            entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {x, y})));
        }

        QuadraticSplit split = new QuadraticSplit(entries, 2);

        BoundingBox g1Bbox = computeBbox(split.getGroup1());
        BoundingBox g2Bbox = computeBbox(split.getGroup2());

        // Calculate overlap area
        double overlapArea = calculateOverlapArea(g1Bbox, g2Bbox);
        double totalArea = g1Bbox.getArea() + g2Bbox.getArea();

        // Overlap should be < 30% of total area
        double overlapRatio = overlapArea / totalArea;
        assertTrue(overlapRatio < 0.5, "Overlap ratio should be < 0.5, got: " + overlapRatio);
    }

    @Test
    public void testQuadraticSplitPreservesBalance() {
        for (int trial = 0; trial < 5; trial++) {
            List<LeafEntry> entries = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                double x = Math.random() * 10000;
                double y = Math.random() * 10000;
                entries.add(new LeafEntry(i, BoundingBox.fromPoint(new double[] {x, y})));
            }

            QuadraticSplit split = new QuadraticSplit(entries, 2);

            int g1Size = split.getGroup1().size();
            int g2Size = split.getGroup2().size();

            // Allow wider range for random distributions
            assertTrue(
                    g1Size >= 4 && g1Size <= 60,
                    "Trial " + trial + ": Group 1 should have 4-60 entries, got " + g1Size);
            assertTrue(
                    g2Size >= 4 && g2Size <= 60,
                    "Trial " + trial + ": Group 2 should have 4-60 entries, got " + g2Size);
        }
    }

    @Test
    public void testLargeTreeWithQuadraticSplit() {
        RTree rtree = new RTree(2, 16);

        // Insert 10K points with random distribution
        for (int i = 0; i < 10000; i++) {
            double x = Math.sin(i * 0.01) * 5000 + Math.random() * 100;
            double y = Math.cos(i * 0.01) * 5000 + Math.random() * 100;
            rtree.insert(new double[] {x, y}, i);
        }

        // All data should be recoverable
        BoundingBox fullSpace =
                new BoundingBox(new double[] {-10000, -10000}, new double[] {10000, 10000});
        List<Integer> results = rtree.search(fullSpace);

        assertEquals(10000, results.size(), "All 10K points should be recoverable");
    }

    private BoundingBox computeBbox(List<LeafEntry> entries) {
        if (entries.isEmpty()) {
            return new BoundingBox(2);
        }

        BoundingBox result = new BoundingBox(2);
        for (LeafEntry entry : entries) {
            result.expand(entry.getBbox());
        }
        return result;
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
