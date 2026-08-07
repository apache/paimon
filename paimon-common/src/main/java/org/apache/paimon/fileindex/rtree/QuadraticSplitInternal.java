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

/** Quadratic split algorithm for internal R-Tree nodes. */
public class QuadraticSplitInternal {

    private final List<RTreeNode> group1 = new ArrayList<>();
    private final List<RTreeNode> group2 = new ArrayList<>();
    private final int dimensions;

    /**
     * Creates a quadratic split of the given child nodes.
     *
     * <p>Same algorithm as leaf split, but operates on RTreeNode children.
     */
    public QuadraticSplitInternal(List<RTreeNode> children, int dimensions) {
        this.dimensions = dimensions;

        if (children.isEmpty()) {
            return;
        }

        if (children.size() == 1) {
            group1.add(children.get(0));
            return;
        }

        // Step 1: Pick seeds
        int[] seeds = pickSeeds(children);
        group1.add(children.get(seeds[0]));
        group2.add(children.get(seeds[1]));

        // Step 2: Assign remaining children
        Set<Integer> assigned = new HashSet<>();
        assigned.add(seeds[0]);
        assigned.add(seeds[1]);

        for (int i = 0; i < children.size(); i++) {
            if (!assigned.contains(i)) {
                assignChild(children.get(i));
                assigned.add(i);
            }
        }
    }

    /** Pick seeds using maximum distance heuristic. */
    private int[] pickSeeds(List<RTreeNode> children) {
        double maxDistance = -1;
        int seed1 = 0;
        int seed2 = 1;

        for (int i = 0; i < children.size(); i++) {
            for (int j = i + 1; j < children.size(); j++) {
                double distance =
                        calculateDistance(
                                children.get(i).getBoundingBox(), children.get(j).getBoundingBox());
                if (distance > maxDistance) {
                    maxDistance = distance;
                    seed1 = i;
                    seed2 = j;
                }
            }
        }

        return new int[] {seed1, seed2};
    }

    /** Calculate distance between two bounding boxes. */
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

    /** Assign child to group with minimum expansion. */
    private void assignChild(RTreeNode child) {
        BoundingBox g1Bbox = computeBbox(group1);
        BoundingBox g2Bbox = computeBbox(group2);

        double expansion1 = g1Bbox.getExpansionArea(child.getBoundingBox());
        double expansion2 = g2Bbox.getExpansionArea(child.getBoundingBox());

        if (expansion1 < expansion2
                || (Math.abs(expansion1 - expansion2) < 1e-9
                        && g1Bbox.getArea() < g2Bbox.getArea())) {
            group1.add(child);
        } else {
            group2.add(child);
        }
    }

    /** Compute bounding box for a group of nodes. */
    private BoundingBox computeBbox(List<RTreeNode> nodes) {
        if (nodes.isEmpty()) {
            return new BoundingBox(dimensions);
        }

        BoundingBox result = new BoundingBox(dimensions);
        for (RTreeNode node : nodes) {
            result.expand(node.getBoundingBox());
        }
        return result;
    }

    public List<RTreeNode> getGroup1() {
        return group1;
    }

    public List<RTreeNode> getGroup2() {
        return group2;
    }
}
