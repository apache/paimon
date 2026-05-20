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

/** R-Tree spatial index implementation. */
public class RTree {
    private RTreeNode root;
    private final int dimensions;
    private final int minEntries;
    private final int maxEntries;
    private int size;

    public RTree(int dimensions, int maxEntries) {
        this.dimensions = dimensions;
        this.maxEntries = maxEntries;
        this.minEntries = Math.max(2, maxEntries / 2);
        this.root = new RTreeNode(dimensions, maxEntries, true);
        this.size = 0;
    }

    public int getDimensions() {
        return dimensions;
    }

    public int getMinEntries() {
        return minEntries;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public RTreeNode getRoot() {
        return root;
    }

    public int getSize() {
        return size;
    }

    public void insert(double[] point, int rowId) {
        BoundingBox pointBox = BoundingBox.fromPoint(point);
        insert(pointBox, rowId, root);
        size++;
    }

    private void insert(BoundingBox bbox, int rowId, RTreeNode node) {
        if (node.isLeaf()) {
            node.addRowId(rowId);
            node.getBoundingBox().expand(bbox);

            if (node.canSplit()) {
                splitNode(node);
            }
        } else {
            RTreeNode bestChild = chooseBestChild(node, bbox);
            insert(bbox, rowId, bestChild);

            if (bestChild.canSplit()) {
                splitNode(bestChild);
            }
            node.getBoundingBox().expand(bbox);
        }
    }

    private RTreeNode chooseBestChild(RTreeNode node, BoundingBox bbox) {
        RTreeNode best = null;
        double minExpansion = Double.POSITIVE_INFINITY;
        double minArea = Double.POSITIVE_INFINITY;

        for (RTreeNode child : node.getChildren()) {
            double expansion = child.getBoundingBox().getExpansionArea(bbox);
            double area = child.getBoundingBox().getArea();

            if (expansion < minExpansion || (expansion == minExpansion && area < minArea)) {
                minExpansion = expansion;
                minArea = area;
                best = child;
            }
        }

        return best;
    }

    private void splitNode(RTreeNode node) {
        List<Integer> rowIds = new ArrayList<>(node.getLeafRowIds());
        List<RTreeNode> children = new ArrayList<>(node.getChildren());

        node.getLeafRowIds().clear();
        node.getChildren().clear();

        if (node.isLeaf()) {
            RTreeNode newNode = new RTreeNode(dimensions, maxEntries, true);
            distributeLeafEntries(rowIds, node, newNode);

            if (node == root) {
                RTreeNode newRoot = new RTreeNode(dimensions, maxEntries, false);
                newRoot.addChild(node);
                newRoot.addChild(newNode);
                root = newRoot;
            } else {
                adjustParent(node, newNode);
            }
        } else {
            RTreeNode newNode = new RTreeNode(dimensions, maxEntries, false);
            distributeInternalEntries(children, node, newNode);

            if (node == root) {
                RTreeNode newRoot = new RTreeNode(dimensions, maxEntries, false);
                newRoot.addChild(node);
                newRoot.addChild(newNode);
                root = newRoot;
            } else {
                adjustParent(node, newNode);
            }
        }
    }

    private void distributeLeafEntries(List<Integer> rowIds, RTreeNode node1, RTreeNode node2) {
        int mid = rowIds.size() / 2;
        for (int i = 0; i < mid; i++) {
            node1.addRowId(rowIds.get(i));
        }
        for (int i = mid; i < rowIds.size(); i++) {
            node2.addRowId(rowIds.get(i));
        }
    }

    private void distributeInternalEntries(
            List<RTreeNode> children, RTreeNode node1, RTreeNode node2) {
        int mid = children.size() / 2;
        for (int i = 0; i < mid; i++) {
            node1.addChild(children.get(i));
        }
        for (int i = mid; i < children.size(); i++) {
            node2.addChild(children.get(i));
        }
    }

    private void adjustParent(RTreeNode node, RTreeNode newNode) {
        // In a real implementation, we would find parent and adjust
        // For now, this is a simplified version
    }

    public List<Integer> search(BoundingBox searchBox) {
        List<Integer> results = new ArrayList<>();
        search(searchBox, root, results);
        return results;
    }

    private void search(BoundingBox searchBox, RTreeNode node, List<Integer> results) {
        if (!node.getBoundingBox().intersects(searchBox)) {
            return;
        }

        if (node.isLeaf()) {
            for (Integer rowId : node.getLeafRowIds()) {
                results.add(rowId);
            }
        } else {
            for (RTreeNode child : node.getChildren()) {
                search(searchBox, child, results);
            }
        }
    }

    public List<Integer> search(double[] point) {
        BoundingBox pointBox = BoundingBox.fromPoint(point);
        return search(pointBox);
    }
}
