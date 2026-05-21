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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Quadratic split algorithm for R-Tree node splitting. */
public class QuadraticSplit {

    private final List<LeafEntry> group1 = new ArrayList<>();
    private final List<LeafEntry> group2 = new ArrayList<>();
    private final int dimensions;

    /**
     * Creates a quadratic split of the given entries.
     *
     * <p>Algorithm:
     *
     * <ol>
     *   <li>Pick seeds: Find two entries with maximum distance
     *   <li>Assign others: Assign each remaining entry to group with minimum expansion
     * </ol>
     */
    public QuadraticSplit(List<LeafEntry> entries, int dimensions) {
        this.dimensions = dimensions;

        if (entries.isEmpty()) {
            return;
        }

        if (entries.size() == 1) {
            group1.add(entries.get(0));
            return;
        }

        // Step 1: Pick seeds (two entries with maximum distance)
        int[] seeds = pickSeeds(entries);
        group1.add(entries.get(seeds[0]));
        group2.add(entries.get(seeds[1]));

        // Step 2: Assign remaining entries
        Set<Integer> assigned = new HashSet<>();
        assigned.add(seeds[0]);
        assigned.add(seeds[1]);

        for (int i = 0; i < entries.size(); i++) {
            if (!assigned.contains(i)) {
                assignEntry(entries.get(i));
                assigned.add(i);
            }
        }
    }

    /**
     * Pick seeds using maximum distance heuristic.
     *
     * <p>Find two entries whose bounding boxes have maximum separation distance.
     */
    private int[] pickSeeds(List<LeafEntry> entries) {
        double maxDistance = -1;
        int seed1 = 0;
        int seed2 = 1;

        for (int i = 0; i < entries.size(); i++) {
            for (int j = i + 1; j < entries.size(); j++) {
                double distance =
                        calculateDistance(entries.get(i).getBbox(), entries.get(j).getBbox());
                if (distance > maxDistance) {
                    maxDistance = distance;
                    seed1 = i;
                    seed2 = j;
                }
            }
        }

        return new int[] {seed1, seed2};
    }

    /**
     * Calculate distance between two bounding boxes.
     *
     * <p>Uses Euclidean distance between box centers.
     */
    private double calculateDistance(BoundingBox b1, BoundingBox b2) {
        double distance = 0;
        double[] min1 = b1.getMin();
        double[] max1 = b1.getMax();
        double[] min2 = b2.getMin();
        double[] max2 = b2.getMax();

        for (int i = 0; i < dimensions; i++) {
            double c1 = (min1[i] + max1[i]) / 2.0;
            double c2 = (min2[i] + max2[i]) / 2.0;
            distance += Math.pow(c1 - c2, 2);
        }

        return Math.sqrt(distance);
    }

    /** Assign entry to group with minimum expansion. */
    private void assignEntry(LeafEntry entry) {
        BoundingBox g1Bbox = computeBbox(group1);
        BoundingBox g2Bbox = computeBbox(group2);

        double expansion1 = g1Bbox.getExpansionArea(entry.getBbox());
        double expansion2 = g2Bbox.getExpansionArea(entry.getBbox());

        // Tie-breaking: prefer group with smaller area
        if (expansion1 < expansion2
                || (Math.abs(expansion1 - expansion2) < 1e-9
                        && g1Bbox.getArea() < g2Bbox.getArea())) {
            group1.add(entry);
        } else {
            group2.add(entry);
        }
    }

    /** Compute bounding box for a group of entries. */
    private BoundingBox computeBbox(List<LeafEntry> entries) {
        if (entries.isEmpty()) {
            return new BoundingBox(dimensions);
        }

        BoundingBox result = new BoundingBox(dimensions);
        for (LeafEntry entry : entries) {
            result.expand(entry.getBbox());
        }
        return result;
    }

    public List<LeafEntry> getGroup1() {
        return group1;
    }

    public List<LeafEntry> getGroup2() {
        return group2;
    }
}
